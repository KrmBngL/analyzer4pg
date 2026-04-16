"""
index_advisor.py - Index recommendations for analyzer4pg
Analyzes the execution plan and existing indexes to suggest CREATE INDEX statements.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Set, Tuple

from .plan_analyzer import (
    PlanResult,
    PlanNode,
    extract_seq_scan_nodes,
    extract_columns_from_condition,
    _walk_nodes,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class IndexRecommendation:
    """A single index recommendation."""
    priority: str           # HIGH | MEDIUM | LOW
    table: str
    schema: str
    columns: List[str]
    index_type: str         # btree | hash | gin | gist | brin
    is_partial: bool
    partial_where: Optional[str]
    reason: str
    impact: str
    ddl: str                # Ready-to-execute CREATE INDEX statement
    estimated_improvement: str


@dataclass
class UnusedIndexWarning:
    """An existing index that appears to be unused."""
    schema: str
    table: str
    index_name: str
    index_size: str
    index_def: str
    recommendation: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SQL_KEYWORDS: Set[str] = {
    "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "IS", "IN",
    "ANY", "ALL", "LIKE", "ILIKE", "BETWEEN", "EXISTS",
    "CASE", "WHEN", "THEN", "ELSE", "END", "FROM", "WHERE",
    "SELECT", "JOIN", "ON", "AS", "BY", "ORDER", "GROUP",
    "HAVING", "LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT",
}


def _clean_column_name(name: str) -> str:
    """Strip casts and whitespace from a column name."""
    name = re.sub(r"::[a-zA-Z_]+(\[\])?", "", name).strip()
    # Remove table alias prefix
    if "." in name:
        name = name.split(".")[-1]
    return name.strip("() ")


def _deduplicate_recommendations(recs: List[IndexRecommendation]) -> List[IndexRecommendation]:
    """Remove duplicate recommendations on (schema, table, frozenset(columns))."""
    seen: Set[tuple] = set()
    result = []
    for rec in recs:
        key = (rec.schema, rec.table, frozenset(rec.columns))
        if key not in seen:
            seen.add(key)
            result.append(rec)
    return result


def _make_index_name(schema: str, table: str, columns: List[str], partial: bool = False) -> str:
    col_part = "_".join(c[:10] for c in columns[:4])
    suffix = "_partial" if partial else ""
    return f"idx_{table[:20]}_{col_part}{suffix}"


def _make_ddl(
    schema: str,
    table: str,
    columns: List[str],
    index_type: str = "btree",
    partial_where: Optional[str] = None,
) -> str:
    index_name = _make_index_name(schema, table, columns, partial=bool(partial_where))
    col_list = ", ".join(columns)
    qualified = f"{schema}.{table}" if schema not in ("public", "") else table
    using = f" USING {index_type}" if index_type != "btree" else ""
    where_clause = f"\n  WHERE {partial_where}" if partial_where else ""
    return f"CREATE INDEX CONCURRENTLY {index_name}\n  ON {qualified}{using} ({col_list}){where_clause};"


def _columns_covered_by_existing(
    columns: List[str], existing_indexes: List[dict]
) -> bool:
    """Return True if the first column(s) of a recommended index are already indexed."""
    for idx in existing_indexes:
        idx_cols_raw = idx.get("columns", "")
        idx_cols = [_clean_column_name(c) for c in idx_cols_raw.split(",")]
        # Check if all recommended columns are a prefix of this index
        if len(idx_cols) >= len(columns):
            if all(
                idx_cols[i].lower() == columns[i].lower()
                for i in range(len(columns))
            ):
                return True
    return False


# ---------------------------------------------------------------------------
# Core advisor logic
# ---------------------------------------------------------------------------

class IndexAdvisor:
    """
    Analyzes a PlanResult and existing index information to produce
    actionable CREATE INDEX recommendations.
    """

    # Minimum rows for a table before we suggest an index
    MIN_ROWS_FOR_INDEX = 500

    def advise(
        self,
        plan_result: PlanResult,
        db_conn=None,
    ) -> Tuple[List[IndexRecommendation], List[UnusedIndexWarning]]:
        """
        Main entry point. Returns (recommendations, unused_index_warnings).
        db_conn is optional but enables richer advice (existing index checks,
        table size checks, unused index detection).
        """
        recommendations: List[IndexRecommendation] = []
        unused_warnings: List[UnusedIndexWarning] = []

        # Collect table -> existing indexes mapping
        existing_indexes: Dict[str, List[dict]] = {}
        table_stats: Dict[str, dict] = {}

        if db_conn:
            tables = self._collect_tables(plan_result)
            for schema, table in tables:
                key = f"{schema}.{table}"
                existing_indexes[key] = db_conn.fetch_existing_indexes(schema, table)
                table_stats[key] = db_conn.fetch_table_stats(schema, table)

            # Unused index detection
            for row in db_conn.fetch_unused_indexes():
                unused_warnings.append(UnusedIndexWarning(
                    schema=row["schemaname"],
                    table=row["table_name"],
                    index_name=row["index_name"],
                    index_size=row["index_size"],
                    index_def=row["index_def"],
                    recommendation=(
                        f"This index has never been used (idx_scan = 0). "
                        f"Consider dropping it to reduce write overhead and storage:\n"
                        f"  DROP INDEX CONCURRENTLY {row['index_name']};"
                    ),
                ))

        # --- 1. Sequential scan recommendations ---
        for node in extract_seq_scan_nodes(plan_result):
            recs = self._advise_for_seq_scan(
                node,
                existing_indexes.get(
                    f"{node.schema or 'public'}.{node.relation_name}", []
                ),
                table_stats.get(
                    f"{node.schema or 'public'}.{node.relation_name}", {}
                ),
            )
            recommendations.extend(recs)

        # --- 2. Join condition recommendations ---
        for node in _walk_nodes(plan_result.root_node):
            if node.node_type in ("Hash Join", "Merge Join", "Nested Loop"):
                recs = self._advise_for_join(node, existing_indexes)
                recommendations.extend(recs)

        # --- 3. Sort recommendations ---
        for node in _walk_nodes(plan_result.root_node):
            if node.node_type in ("Sort", "Incremental Sort") and node.sort_key:
                # Find the table being sorted (look at child)
                recs = self._advise_for_sort(node, plan_result, existing_indexes)
                recommendations.extend(recs)

        recommendations = _deduplicate_recommendations(recommendations)
        recommendations.sort(key=lambda r: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[r.priority])

        return recommendations, unused_warnings

    # ------------------------------------------------------------------
    def _collect_tables(self, plan_result: PlanResult) -> List[Tuple[str, str]]:
        tables = []
        seen = set()
        for node in _walk_nodes(plan_result.root_node):
            if node.relation_name:
                schema = node.schema or "public"
                key = (schema, node.relation_name)
                if key not in seen:
                    seen.add(key)
                    tables.append(key)
        return tables

    # ------------------------------------------------------------------
    def _advise_for_seq_scan(
        self,
        node: PlanNode,
        existing_indexes: List[dict],
        table_stats: dict,
    ) -> List[IndexRecommendation]:
        recommendations = []
        schema = node.schema or "public"
        table = node.relation_name

        rows = node.actual_rows if node.has_actual else node.plan_rows
        if rows < self.MIN_ROWS_FOR_INDEX:
            return []  # Table is small, seq scan is fine

        # Extract columns from filter conditions
        filter_columns = []
        for cond in [node.filter, node.index_cond, node.recheck_cond]:
            filter_columns.extend(extract_columns_from_condition(cond or ""))
        filter_columns = list(dict.fromkeys(filter_columns))  # dedup

        if not filter_columns:
            return []

        # Check if columns are already indexed
        if _columns_covered_by_existing(filter_columns[:1], existing_indexes):
            # First column is indexed - maybe a composite index would be better
            if len(filter_columns) > 1 and not _columns_covered_by_existing(
                filter_columns, existing_indexes
            ):
                recommendations.append(self._make_recommendation(
                    priority="MEDIUM",
                    schema=schema,
                    table=table,
                    columns=filter_columns,
                    reason=(
                        f"Seq scan on '{table}' with multi-column filter. "
                        f"A composite index on ({', '.join(filter_columns)}) "
                        "would allow index-only scans."
                    ),
                    impact=(
                        f"Could eliminate the sequential scan of {rows:,} rows. "
                        "Estimated 50-90% query time reduction."
                    ),
                    node=node,
                ))
            return recommendations

        # Determine if a partial index makes sense
        # Simple heuristic: if filter has a constant equality on a low-cardinality column
        partial_where = self._detect_partial_index_condition(node.filter)

        # Single column index (most common case)
        priority = "HIGH" if rows > 10_000 else "MEDIUM"
        for col in filter_columns[:3]:  # limit to top 3 candidates
            col_clean = _clean_column_name(col)
            recommendations.append(self._make_recommendation(
                priority=priority,
                schema=schema,
                table=table,
                columns=[col_clean],
                reason=(
                    f"No index on '{table}.{col_clean}' but it appears in a Filter "
                    f"during a sequential scan of {rows:,} rows.\n"
                    f"  Filter: {node.filter}"
                ),
                impact=(
                    f"Could reduce query time from {node.actual_total_time:.1f}ms "
                    "to near-zero with an index scan."
                    if node.has_actual else
                    f"Could eliminate sequential scan (cost: {node.total_cost:.2f})."
                ),
                node=node,
                partial_where=partial_where,
            ))

        # If multiple filter columns, also suggest a composite index
        if len(filter_columns) > 1:
            cols_clean = [_clean_column_name(c) for c in filter_columns[:3]]
            recommendations.append(self._make_recommendation(
                priority="MEDIUM",
                schema=schema,
                table=table,
                columns=cols_clean,
                reason=(
                    f"Multi-column filter on '{table}'. "
                    f"Composite index on ({', '.join(cols_clean)}) enables index-only scans."
                ),
                impact="Could enable covering/index-only scans, eliminating heap access.",
                node=node,
            ))

        return recommendations

    # ------------------------------------------------------------------
    def _advise_for_join(
        self,
        node: PlanNode,
        existing_indexes: Dict[str, List[dict]],
    ) -> List[IndexRecommendation]:
        recommendations = []
        cond = node.hash_cond or node.merge_cond or node.join_filter
        if not cond:
            return []

        # Extract table.column pairs from join condition
        # Pattern: word.word = word.word
        pairs = re.findall(
            r"\b(?:(\w+)\.)?(\w+)\s*=\s*(?:(\w+)\.)?(\w+)\b",
            cond,
            re.IGNORECASE,
        )
        for t1, c1, t2, c2 in pairs:
            if c1.upper() in _SQL_KEYWORDS or c2.upper() in _SQL_KEYWORDS:
                continue
            # We only have alias/table names from the plan - best effort
            for col, alias in [(c1, t1), (c2, t2)]:
                col_clean = _clean_column_name(col)
                # Find the actual table from child nodes
                table_node = self._find_node_by_alias(node, alias)
                if table_node and table_node.relation_name:
                    schema = table_node.schema or "public"
                    table = table_node.relation_name
                    key = f"{schema}.{table}"
                    idx_list = existing_indexes.get(key, [])
                    if not _columns_covered_by_existing([col_clean], idx_list):
                        recommendations.append(self._make_recommendation(
                            priority="HIGH",
                            schema=schema,
                            table=table,
                            columns=[col_clean],
                            reason=(
                                f"No index on join column '{table}.{col_clean}'. "
                                f"Used in: {cond}"
                            ),
                            impact=(
                                "Indexing join columns can convert nested loop scans "
                                "to efficient index lookups."
                            ),
                            node=node,
                        ))

        return recommendations

    # ------------------------------------------------------------------
    def _advise_for_sort(
        self,
        node: PlanNode,
        plan_result: PlanResult,
        existing_indexes: Dict[str, List[dict]],
    ) -> List[IndexRecommendation]:
        """Suggest indexes to avoid explicit sort operations."""
        recommendations = []
        if not node.sort_key:
            return []

        # Find the table being sorted - look for a scan in children
        scan_node = self._find_scan_below(node)
        if not scan_node or not scan_node.relation_name:
            return []

        schema = scan_node.schema or "public"
        table = scan_node.relation_name
        key = f"{schema}.{table}"
        idx_list = existing_indexes.get(key, [])

        sort_cols = [_clean_column_name(c) for c in node.sort_key]
        # Normalize DESC/ASC
        sort_cols = [re.sub(r"\s+(ASC|DESC|NULLS\s+\w+)$", "", c, flags=re.IGNORECASE) for c in sort_cols]

        if not _columns_covered_by_existing(sort_cols[:1], idx_list):
            is_spill = node.sort_method and "disk" in node.sort_method.lower()
            priority = "HIGH" if is_spill else "LOW"
            recommendations.append(self._make_recommendation(
                priority=priority,
                schema=schema,
                table=table,
                columns=sort_cols[:3],
                reason=(
                    f"Sort on '{table}' for columns ({', '.join(sort_cols)}) "
                    "has no supporting index, requiring an explicit sort step."
                    + (" Sort is spilling to disk!" if is_spill else "")
                ),
                impact=(
                    "An index matching the sort order eliminates the sort step entirely "
                    "(pre-sorted index scan)."
                ),
                node=node,
            ))

        return recommendations

    # ------------------------------------------------------------------

    def _find_node_by_alias(self, node: PlanNode, alias: str) -> Optional[PlanNode]:
        if not alias:
            return None
        for n in _walk_nodes(node):
            if n.alias and n.alias.lower() == alias.lower():
                return n
            if n.relation_name and n.relation_name.lower() == alias.lower():
                return n
        return None

    def _find_scan_below(self, node: PlanNode) -> Optional[PlanNode]:
        for child in node.children:
            for n in _walk_nodes(child):
                if n.node_type in ("Seq Scan", "Index Scan", "Index Only Scan",
                                    "Bitmap Heap Scan"):
                    return n
        return None

    def _detect_partial_index_condition(self, filter_str: Optional[str]) -> Optional[str]:
        """
        Detect a good partial index condition from a filter.
        Example: "(status = 'active')" -> "status = 'active'"
        Only suggest partial if the filter is a simple equality on a string/bool.
        """
        if not filter_str:
            return None
        # Simple equality on string literal: col = 'value'
        m = re.search(
            r"\b([a-z_][a-z0-9_]*)\s*=\s*'([^']+)'",
            filter_str.strip("()"),
            re.IGNORECASE,
        )
        if m:
            col, val = m.group(1), m.group(2)
            if col.upper() not in _SQL_KEYWORDS:
                return f"{col} = '{val}'"
        return None

    def _make_recommendation(
        self,
        priority: str,
        schema: str,
        table: str,
        columns: List[str],
        reason: str,
        impact: str,
        node: PlanNode,
        index_type: str = "btree",
        partial_where: Optional[str] = None,
    ) -> IndexRecommendation:
        ddl = _make_ddl(schema, table, columns, index_type, partial_where)
        return IndexRecommendation(
            priority=priority,
            table=table,
            schema=schema,
            columns=columns,
            index_type=index_type,
            is_partial=bool(partial_where),
            partial_where=partial_where,
            reason=reason,
            impact=impact,
            ddl=ddl,
            estimated_improvement=(
                "HIGH (eliminate seq scan)" if priority == "HIGH"
                else "MEDIUM (reduce I/O)" if priority == "MEDIUM"
                else "LOW (avoid sort step)"
            ),
        )
