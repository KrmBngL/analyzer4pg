"""
rewrite_verify.py - Shared rewrite verification for analyzer4pg.

A textual rewrite (rule-based or AI-suggested) can be syntactically correct
and still be a worse plan -- it depends entirely on the target database's
indexes, table sizes and statistics, which no static rule (or LLM) can know
in advance. So before any rewritten query is ever shown to the user, this
module asks the real database: EXPLAIN (ANALYZE, when the original query was)
the rewrite and only keep it if it's actually cheaper on this database.
Used by both the CLI and the web app so both share one verification path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .plan_analyzer import PlanAnalyzer, PlanResult


@dataclass
class RewriteVerification:
    verified_with: str  # "analyze" (real execution time) | "cost" (planner estimate)
    before: float
    after: float
    improvement_pct: float
    new_plan: PlanResult


def verify_rewrite(db, plan_result: PlanResult, rewritten_sql: str, use_analyze: bool) -> Optional[RewriteVerification]:
    """
    Returns a RewriteVerification if the rewrite is confirmed cheaper on this
    database, or None if it should be discarded (fails to EXPLAIN, or isn't
    at least ~2% cheaper).
    """
    if use_analyze and plan_result.has_actual:
        try:
            alt_plan = db.explain_query(rewritten_sql, use_analyze=True)
            alt_time = alt_plan.get("Execution Time")
        except Exception:
            return None
        original_time = plan_result.execution_time
        if alt_time is None or original_time <= 0 or alt_time >= original_time * 0.98:
            return None
        return RewriteVerification(
            verified_with="analyze",
            before=round(original_time, 3),
            after=round(alt_time, 3),
            improvement_pct=round((original_time - alt_time) / original_time * 100, 1),
            new_plan=PlanAnalyzer().analyze_from_json(alt_plan),
        )

    try:
        alt_plan = db.explain_query(rewritten_sql, use_analyze=False)
        alt_cost = alt_plan.get("Plan", {}).get("Total Cost")
    except Exception:
        return None
    original_cost = plan_result.root_node.total_cost
    if alt_cost is None or original_cost <= 0 or alt_cost >= original_cost * 0.98:
        return None
    return RewriteVerification(
        verified_with="cost",
        before=round(original_cost, 2),
        after=round(alt_cost, 2),
        improvement_pct=round((original_cost - alt_cost) / original_cost * 100, 1),
        new_plan=PlanAnalyzer().analyze_from_json(alt_plan),
    )
