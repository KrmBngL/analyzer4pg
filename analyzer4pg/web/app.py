"""
app.py - Flask web application for analyzer4pg
Serves the single-page UI and provides analysis API endpoints.
"""

from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory

from ..connection import DatabaseConnection, build_connection_config
from ..plan_analyzer import PlanAnalyzer
from ..index_advisor import IndexAdvisor
from ..query_advisor import QueryAdvisor, format_sql

STATIC_DIR = Path(__file__).parent / "static"

app = Flask(__name__, static_folder=str(STATIC_DIR))
app.config["JSON_SORT_KEYS"] = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(data: dict) -> DatabaseConnection:
    cfg = build_connection_config(
        host=data.get("host", "localhost"),
        port=int(data.get("port", 5432)),
        dbname=data.get("dbname", "postgres"),
        user=data.get("user", "postgres"),
        password=data.get("password") or None,
        sslmode=data.get("sslmode", "prefer"),
    )
    db = DatabaseConnection(cfg)
    db.connect()
    schema = (data.get("schema") or "").strip()
    if schema:
        db.set_search_path(schema)
    return db


def _verify_rewrite(db: DatabaseConnection, original_cost: float, rewritten_sql: str):
    """
    A textual rewrite can be syntactically correct and still be a worse plan —
    it depends entirely on the target database's indexes, table sizes and
    statistics, which no static rule can know in advance. So before a rewrite
    is ever shown, ask the real planner: EXPLAIN both queries and only keep
    the rewrite if it's actually cheaper. Also doubles as a safety net against
    a rewrite that doesn't even parse against the live schema.
    Returns a cost-comparison dict, or None if the rewrite should be discarded.
    """
    try:
        alt_plan = db.explain_query(rewritten_sql, use_analyze=False)
        alt_cost = alt_plan.get("Plan", {}).get("Total Cost")
    except Exception:
        return None
    if alt_cost is None or original_cost <= 0 or alt_cost >= original_cost * 0.98:
        return None
    return {
        "estimated_cost_before": round(original_cost, 2),
        "estimated_cost_after": round(alt_cost, 2),
        "cost_improvement_pct": round((original_cost - alt_cost) / original_cost * 100, 1),
    }


def _node_to_dict(node) -> dict:
    """Recursively convert a PlanNode to a JSON-serialisable dict."""
    row_est_ratio = None
    if node.has_actual and node.plan_rows > 0:
        row_est_ratio = round(node.row_estimation_ratio, 2)

    return {
        "node_type": node.node_type,
        "relation_name": node.relation_name,
        "alias": node.alias,
        "schema": node.schema,
        "index_name": node.index_name,
        "startup_cost": node.startup_cost,
        "total_cost": node.total_cost,
        "plan_rows": node.plan_rows,
        "actual_rows": node.actual_rows if node.has_actual else None,
        "actual_total_time": round(node.actual_total_time, 3) if node.has_actual else None,
        "actual_loops": node.actual_loops,
        "has_actual": node.has_actual,
        "filter": node.filter,
        "index_cond": node.index_cond,
        "hash_cond": node.hash_cond,
        "join_filter": node.join_filter,
        "sort_key": node.sort_key,
        "sort_method": node.sort_method,
        "sort_space_used": node.sort_space_used,
        "sort_space_type": node.sort_space_type,
        "hash_batches": node.hash_batches,
        "peak_memory_usage": node.peak_memory_usage,
        "shared_hit_blocks": node.shared_hit_blocks,
        "shared_read_blocks": node.shared_read_blocks,
        "rows_removed_by_filter": node.rows_removed_by_filter,
        "parallel_aware": node.parallel_aware,
        "workers_planned": node.workers_planned,
        "row_estimation_ratio": row_est_ratio,
        "children": [_node_to_dict(c) for c in node.children],
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory(str(STATIC_DIR), "index.html")


@app.route("/api/schemas", methods=["POST"])
def get_schemas():
    data = request.get_json(force=True) or {}
    try:
        db = _make_db(data)
        schemas = db.fetch_schemas()
        db.close()
        return jsonify({"success": True, "schemas": schemas})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 400


@app.route("/api/test-connection", methods=["POST"])
def test_connection():
    data = request.get_json(force=True) or {}
    try:
        db = _make_db(data)
        info = {
            "success": True,
            "server_version": db.server_version,
            "database": db.get_current_database(),
        }
        db.close()
        return jsonify(info)
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 400


@app.route("/api/analyze", methods=["POST"])
def analyze():
    data = request.get_json(force=True) or {}
    sql = (data.get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "SQL sorgusu boş"}), 400

    use_analyze = data.get("use_analyze", True)

    try:
        db = _make_db(data)
    except Exception as exc:
        return jsonify({"error": f"Bağlantı hatası: {exc}"}), 400

    try:
        plan_result  = PlanAnalyzer().analyze(db, sql, use_analyze=use_analyze)
        index_recs, unused = IndexAdvisor().advise(plan_result, db_conn=db)
        query_recs   = QueryAdvisor().advise(sql, db_conn=db)

        # A rewritten_sql is only ever shown if EXPLAIN confirms it's actually
        # cheaper on this database — see _verify_rewrite for why that matters.
        rewrite_costs = {}
        for i, r in enumerate(query_recs):
            if not r.rewritten_sql:
                continue
            info = _verify_rewrite(db, plan_result.root_node.total_cost, r.rewritten_sql)
            if info is None:
                r.rewritten_sql = None
            else:
                rewrite_costs[i] = info

        # Recalculate final score including query advisor deductions
        query_deduction = sum(r.score_impact for r in query_recs)
        final_score = max(0, plan_result.score - query_deduction)
        for threshold, grade in [(90,"A"),(75,"B"),(60,"C"),(40,"D"),(0,"F")]:
            if final_score >= threshold:
                final_grade = grade
                break

        return jsonify({
            "score": final_score,
            "grade": final_grade,
            "planning_time": plan_result.planning_time,
            "execution_time": plan_result.execution_time,
            "has_actual": plan_result.has_actual,
            "formatted_sql": format_sql(sql),
            "plan_tree": _node_to_dict(plan_result.root_node),
            "findings": [
                {
                    "level": f.level,
                    "category": f.category,
                    "title": f.title,
                    "description": f.description,
                    "recommendation": f.recommendation,
                    "node_type": f.node.node_type if f.node else None,
                    "relation_name": f.node.relation_name if f.node else None,
                    "score_impact": f.score_impact,
                }
                for f in plan_result.findings
            ],
            "index_recommendations": [
                {
                    "priority": r.priority,
                    "schema": r.schema,
                    "table": r.table,
                    "columns": r.columns,
                    "ddl": r.ddl,
                    "reason": r.reason.split("\n")[0],
                    "impact": r.impact.split("\n")[0],
                    "is_partial": r.is_partial,
                    "estimated_improvement": r.estimated_improvement,
                }
                for r in index_recs
            ],
            "unused_indexes": [
                {
                    "schema": u.schema,
                    "table": u.table,
                    "index_name": u.index_name,
                    "index_size": u.index_size,
                    "index_def": u.index_def,
                }
                for u in unused
            ],
            "query_recommendations": [
                {
                    "priority": r.priority,
                    "category": r.category,
                    "title": r.title,
                    "description": r.description,
                    "example_before": r.example_before,
                    "example_after": r.example_after,
                    "score_impact": r.score_impact,
                    "rewritten_sql": r.rewritten_sql,
                    "estimated_cost_before": rewrite_costs.get(i, {}).get("estimated_cost_before"),
                    "estimated_cost_after": rewrite_costs.get(i, {}).get("estimated_cost_after"),
                    "cost_improvement_pct": rewrite_costs.get(i, {}).get("cost_improvement_pct"),
                }
                for i, r in enumerate(query_recs)
            ],
        })

    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"Analiz hatası: {exc}"}), 500
    finally:
        db.close()
