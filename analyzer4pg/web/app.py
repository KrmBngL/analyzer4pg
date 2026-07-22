"""
app.py - Flask web application for analyzer4pg
Serves the single-page UI and provides analysis API endpoints.
"""

from __future__ import annotations

from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory

from ..connection import DatabaseConnection, build_connection_config, QuerySyntaxError
from ..plan_analyzer import PlanAnalyzer
from ..index_advisor import IndexAdvisor
from ..query_advisor import QueryAdvisor, format_sql
from ..rewrite_verify import verify_rewrite
from ..llm_advisor import LLMAdvisor

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


def _verify_rewrite(db: DatabaseConnection, plan_result, rewritten_sql: str, use_analyze: bool):
    """
    Thin adapter over rewrite_verify.verify_rewrite() that keeps this route's
    existing JSON field names (time_before_ms/cost_before/...) stable.
    """
    v = verify_rewrite(db, plan_result, rewritten_sql, use_analyze)
    if v is None:
        return None
    info = {
        "verified_with": v.verified_with,
        "improvement_pct": v.improvement_pct,
        "new_plan": _plan_result_to_dict(v.new_plan),
    }
    if v.verified_with == "analyze":
        info["time_before_ms"] = v.before
        info["time_after_ms"] = v.after
    else:
        info["cost_before"] = v.before
        info["cost_after"] = v.after
    return info


def _summarize_plan(plan_result) -> str:
    root = plan_result.root_node
    time_part = f", gerçek süre {plan_result.execution_time:.1f}ms" if plan_result.has_actual else ""
    return f"Kök düğüm: {root.node_type}, planlayıcı maliyeti {root.total_cost:.1f}{time_part}"


def _summarize_findings(findings) -> str:
    top = [f for f in findings if f.level in ("CRITICAL", "WARNING")][:5]
    if not top:
        return ""
    return "\n".join(
        f"- [{f.level}] {f.title}: {f.description.splitlines()[0]}" for f in top
    )


def _try_ai_rewrite(db: DatabaseConnection, sql: str, plan_result, use_analyze: bool):
    """
    Asks Claude for one alternative query structure (e.g. subquery -> CTE)
    beyond the fixed set of mechanical rewrites in query_advisor.py, then
    verifies it the same way as rule-based rewrites: real EXPLAIN (ANALYZE)
    on this database. Returns None if AI is unavailable, declines, or the
    suggestion doesn't verify as actually cheaper.
    """
    advisor = LLMAdvisor()
    if advisor.unavailable_reason():
        return None
    findings_summary = _summarize_findings(plan_result.findings)
    if not findings_summary:
        return None  # nothing worth improving
    try:
        suggestion = advisor.suggest_rewrite(
            sql, _summarize_plan(plan_result), findings_summary, db.server_version
        )
    except RuntimeError:
        return None
    if suggestion is None:
        return None
    verification = verify_rewrite(db, plan_result, suggestion.corrected_sql, use_analyze)
    if verification is None:
        return None
    return {
        "explanation": suggestion.explanation,
        "rewritten_sql": suggestion.corrected_sql,
        "verified_with": verification.verified_with,
        "before": verification.before,
        "after": verification.after,
        "improvement_pct": verification.improvement_pct,
        "new_plan": _plan_result_to_dict(verification.new_plan),
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


def _findings_to_list(findings) -> list:
    return [
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
        for f in findings
    ]


def _plan_result_to_dict(plan_result) -> dict:
    return {
        "score": plan_result.score,
        "grade": plan_result.grade,
        "planning_time": plan_result.planning_time,
        "execution_time": plan_result.execution_time,
        "has_actual": plan_result.has_actual,
        "plan_tree": _node_to_dict(plan_result.root_node),
        "findings": _findings_to_list(plan_result.findings),
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
    use_ai = bool(data.get("use_ai", False))

    try:
        db = _make_db(data)
    except Exception as exc:
        return jsonify({"error": f"Bağlantı hatası: {exc}"}), 400

    ai_fix = None
    try:
        try:
            plan_result = PlanAnalyzer().analyze(db, sql, use_analyze=use_analyze)
        except QuerySyntaxError as exc:
            # A real PostgreSQL syntax error (SQLSTATE 42601) — not an
            # anti-pattern, the query doesn't even parse. Only Claude can
            # attempt a repair here, and only if the caller opted in.
            if not use_ai:
                raise
            advisor = LLMAdvisor()
            reason = advisor.unavailable_reason()
            if reason:
                return jsonify({"error": str(exc), "ai_unavailable": reason}), 400
            suggestion = advisor.fix_syntax_error(sql, str(exc), db.server_version)
            if suggestion is None:
                return jsonify({
                    "error": str(exc),
                    "ai_note": "AI güvenilir bir düzeltme öneremedi.",
                }), 400
            try:
                plan_result = PlanAnalyzer().analyze(db, suggestion.corrected_sql, use_analyze=use_analyze)
            except Exception as exc2:
                return jsonify({
                    "error": str(exc),
                    "ai_note": f"AI'nin önerdiği düzeltme de hata verdi: {exc2}",
                    "ai_suggested_sql": suggestion.corrected_sql,
                }), 400
            # Verified: the corrected query actually EXPLAINs successfully.
            # Continue the whole pipeline on the fixed SQL.
            ai_fix = {
                "original_sql": sql,
                "corrected_sql": suggestion.corrected_sql,
                "explanation": suggestion.explanation,
            }
            sql = suggestion.corrected_sql

        index_recs, unused = IndexAdvisor().advise(plan_result, db_conn=db)
        query_recs   = QueryAdvisor().advise(sql, db_conn=db)

        # A rewritten_sql is only ever shown if EXPLAIN (ANALYZE, when the
        # original query was) confirms it's actually cheaper on this database
        # — see rewrite_verify.verify_rewrite for why that matters.
        rewrite_costs = {}
        for i, r in enumerate(query_recs):
            if not r.rewritten_sql:
                continue
            info = _verify_rewrite(db, plan_result, r.rewritten_sql, use_analyze)
            if info is None:
                r.rewritten_sql = None
            else:
                rewrite_costs[i] = info

        # Optional: ask Claude for a general rewrite (e.g. subquery -> CTE)
        # beyond the fixed rule-based patterns. Verified the same way.
        ai_recommendation = _try_ai_rewrite(db, sql, plan_result, use_analyze) if use_ai else None

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
            "ai_fix": ai_fix,
            "ai_recommendation": ai_recommendation,
            "plan_tree": _node_to_dict(plan_result.root_node),
            "findings": _findings_to_list(plan_result.findings),
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
                    "verified_with": rewrite_costs.get(i, {}).get("verified_with"),
                    "time_before_ms": rewrite_costs.get(i, {}).get("time_before_ms"),
                    "time_after_ms": rewrite_costs.get(i, {}).get("time_after_ms"),
                    "cost_before": rewrite_costs.get(i, {}).get("cost_before"),
                    "cost_after": rewrite_costs.get(i, {}).get("cost_after"),
                    "improvement_pct": rewrite_costs.get(i, {}).get("improvement_pct"),
                    "new_plan": rewrite_costs.get(i, {}).get("new_plan"),
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
