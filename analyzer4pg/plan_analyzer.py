"""
plan_analyzer.py - EXPLAIN plan parsing and performance issue detection for analyzer4pg
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional, List, Iterator


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PlanNode:
    """Represents a single node in the PostgreSQL execution plan tree."""
    node_type: str
    depth: int = 0

    # Cost estimates (from planner)
    startup_cost: float = 0.0
    total_cost: float = 0.0
    plan_rows: int = 0
    plan_width: int = 0

    # Actual execution (only present with ANALYZE)
    actual_startup_time: float = 0.0
    actual_total_time: float = 0.0
    actual_rows: int = 0
    actual_loops: int = 1

    # Buffer usage
    shared_hit_blocks: int = 0
    shared_read_blocks: int = 0
    shared_dirtied_blocks: int = 0
    shared_written_blocks: int = 0
    local_hit_blocks: int = 0
    local_read_blocks: int = 0
    temp_read_blocks: int = 0
    temp_written_blocks: int = 0

    # Scan / join details
    relation_name: Optional[str] = None
    schema: Optional[str] = None
    alias: Optional[str] = None
    index_name: Optional[str] = None
    index_cond: Optional[str] = None
    filter: Optional[str] = None
    rows_removed_by_filter: int = 0
    join_filter: Optional[str] = None
    hash_cond: Optional[str] = None
    merge_cond: Optional[str] = None
    recheck_cond: Optional[str] = None
    join_type: Optional[str] = None

    # Sort details
    sort_key: Optional[List[str]] = None
    sort_method: Optional[str] = None
    sort_space_used: int = 0
    sort_space_type: Optional[str] = None

    # Hash details
    hash_batches: int = 1
    original_hash_batches: int = 1
    peak_memory_usage: int = 0

    # Parallel details
    parallel_aware: bool = False
    workers_planned: int = 0
    workers_launched: int = 0

    # Aggregate
    strategy: Optional[str] = None
    partial_mode: Optional[str] = None

    parent_relationship: Optional[str] = None
    subplan_name: Optional[str] = None

    children: List["PlanNode"] = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    # ---- computed properties -----------------------------------------------

    @property
    def has_actual(self) -> bool:
        return self.actual_loops > 0 and (self.actual_total_time > 0 or self.actual_rows >= 0)

    @property
    def total_actual_time(self) -> float:
        """Total time accounting for loops."""
        return self.actual_total_time * self.actual_loops

    @property
    def total_buffers(self) -> int:
        return self.shared_hit_blocks + self.shared_read_blocks

    @property
    def buffer_hit_ratio(self) -> float:
        total = self.shared_hit_blocks + self.shared_read_blocks
        if total == 0:
            return 1.0
        return self.shared_hit_blocks / total

    @property
    def row_estimation_ratio(self) -> float:
        """actual_rows / plan_rows. 1.0 = perfect estimate."""
        if self.plan_rows == 0:
            return float("inf") if self.actual_rows > 0 else 1.0
        return self.actual_rows / self.plan_rows

    @property
    def cost_fraction(self) -> float:
        """Fraction of root total cost (set externally)."""
        return getattr(self, "_cost_fraction", 0.0)

    @cost_fraction.setter
    def cost_fraction(self, v: float):
        self._cost_fraction = v


# ---------------------------------------------------------------------------

@dataclass
class Finding:
    """A single performance finding from plan analysis."""
    level: str          # CRITICAL | WARNING | INFO
    category: str       # PLAN | INDEX | QUERY | STATISTICS
    title: str
    description: str
    recommendation: str
    node: Optional[PlanNode] = None
    score_impact: int = 0   # points deducted from score


@dataclass
class PlanResult:
    """Full result of analyzing an EXPLAIN output."""
    root_node: PlanNode
    planning_time: float
    execution_time: float
    findings: List[Finding]
    score: int
    grade: str
    has_actual: bool
    all_nodes: List[PlanNode] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _parse_node(data: dict, depth: int = 0) -> PlanNode:
    """Recursively parse an EXPLAIN JSON plan node."""
    node = PlanNode(
        node_type=data.get("Node Type", "Unknown"),
        depth=depth,
        startup_cost=data.get("Startup Cost", 0.0),
        total_cost=data.get("Total Cost", 0.0),
        plan_rows=data.get("Plan Rows", 0),
        plan_width=data.get("Plan Width", 0),
        actual_startup_time=data.get("Actual Startup Time", 0.0),
        actual_total_time=data.get("Actual Total Time", 0.0),
        actual_rows=data.get("Actual Rows", 0),
        actual_loops=data.get("Actual Loops", 1),
        shared_hit_blocks=data.get("Shared Hit Blocks", 0),
        shared_read_blocks=data.get("Shared Read Blocks", 0),
        shared_dirtied_blocks=data.get("Shared Dirtied Blocks", 0),
        shared_written_blocks=data.get("Shared Written Blocks", 0),
        local_hit_blocks=data.get("Local Hit Blocks", 0),
        local_read_blocks=data.get("Local Read Blocks", 0),
        temp_read_blocks=data.get("Temp Read Blocks", 0),
        temp_written_blocks=data.get("Temp Written Blocks", 0),
        relation_name=data.get("Relation Name"),
        schema=data.get("Schema"),
        alias=data.get("Alias"),
        index_name=data.get("Index Name"),
        index_cond=data.get("Index Cond"),
        filter=data.get("Filter"),
        rows_removed_by_filter=data.get("Rows Removed by Filter", 0),
        join_filter=data.get("Join Filter"),
        hash_cond=data.get("Hash Cond"),
        merge_cond=data.get("Merge Cond"),
        recheck_cond=data.get("Recheck Cond"),
        join_type=data.get("Join Type"),
        sort_key=data.get("Sort Key"),
        sort_method=data.get("Sort Method"),
        sort_space_used=data.get("Sort Space Used", 0),
        sort_space_type=data.get("Sort Space Type"),
        hash_batches=data.get("Hash Batches", 1),
        original_hash_batches=data.get("Original Hash Batches", 1),
        peak_memory_usage=data.get("Peak Memory Usage", 0),
        parallel_aware=data.get("Parallel Aware", False),
        workers_planned=data.get("Workers Planned", 0),
        workers_launched=data.get("Workers Launched", 0),
        strategy=data.get("Strategy"),
        partial_mode=data.get("Partial Mode"),
        parent_relationship=data.get("Parent Relationship"),
        subplan_name=data.get("Subplan Name"),
        raw=data,
    )

    for child_data in data.get("Plans", []):
        node.children.append(_parse_node(child_data, depth + 1))

    return node


def _walk_nodes(node: PlanNode) -> Iterator[PlanNode]:
    """DFS iterator over all nodes in the plan tree."""
    yield node
    for child in node.children:
        yield from _walk_nodes(child)


def _annotate_cost_fractions(root: PlanNode) -> None:
    """Set cost_fraction on each node relative to the root total cost."""
    root_cost = root.total_cost if root.total_cost > 0 else 1.0
    for node in _walk_nodes(root):
        node.cost_fraction = node.total_cost / root_cost


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

SEQ_SCAN_ROW_THRESHOLD = 1000          # warn if seq scan returns >= N rows
ROW_ESTIMATION_HIGH_RATIO = 10.0       # warn if actual/estimated >= N
ROW_ESTIMATION_LOW_RATIO = 0.1         # warn if actual/estimated <= N
BUFFER_HIT_RATIO_MIN = 0.80            # warn if hit ratio < 80%
BUFFER_MIN_BLOCKS = 100                # only flag if total blocks >= N
SORT_DISK_KB_THRESHOLD = 0             # any disk sort is bad
HASH_BATCH_THRESHOLD = 1               # warn if batches > 1
LARGE_RESULT_ROWS = 100_000            # warn if returning > N rows without LIMIT


# ---------------------------------------------------------------------------
# Issue detectors
# ---------------------------------------------------------------------------

def _detect_seq_scans(node: PlanNode, findings: List[Finding]) -> None:
    if node.node_type != "Seq Scan" or not node.relation_name:
        return
    rows = node.actual_rows if node.has_actual else node.plan_rows
    if rows >= SEQ_SCAN_ROW_THRESHOLD:
        filter_info = f" with filter: {node.filter}" if node.filter else ""
        findings.append(Finding(
            level="WARNING",
            category="PLAN",
            title=f"Sequential Scan on '{node.relation_name}'",
            description=(
                f"Table '{node.relation_name}' is being fully scanned "
                f"({rows:,} rows processed{filter_info}).\n"
                f"  Cost: {node.total_cost:.2f} | "
                f"Actual time: {node.actual_total_time:.3f}ms" if node.has_actual else
                f"  Cost: {node.total_cost:.2f}"
            ),
            recommendation=(
                "Create an index on the column(s) used in the Filter condition. "
                "See INDEX RECOMMENDATIONS section for specific suggestions."
            ),
            node=node,
            score_impact=10,
        ))


def _detect_estimation_errors(node: PlanNode, findings: List[Finding]) -> None:
    if not node.has_actual or node.plan_rows == 0:
        return
    ratio = node.row_estimation_ratio
    if ratio >= ROW_ESTIMATION_HIGH_RATIO:
        findings.append(Finding(
            level="WARNING",
            category="STATISTICS",
            title=f"Row Under-Estimation in '{node.node_type}'",
            description=(
                f"Planner estimated {node.plan_rows:,} rows but "
                f"actual was {node.actual_rows:,} rows "
                f"({ratio:.1f}x more than expected).\n"
                f"  This causes the planner to choose suboptimal join strategies and index plans."
            ),
            recommendation=(
                f"Run: ANALYZE{(' ' + node.relation_name) if node.relation_name else ''};\n"
                "  If the problem persists, increase statistics target: "
                f"ALTER TABLE {node.relation_name or '<table>'} "
                "ALTER COLUMN <col> SET STATISTICS 500;"
            ),
            node=node,
            score_impact=8,
        ))
    elif ratio <= ROW_ESTIMATION_LOW_RATIO and node.actual_rows > 0:
        findings.append(Finding(
            level="WARNING",
            category="STATISTICS",
            title=f"Row Over-Estimation in '{node.node_type}'",
            description=(
                f"Planner estimated {node.plan_rows:,} rows but "
                f"actual was {node.actual_rows:,} rows "
                f"({1/ratio:.1f}x fewer than expected).\n"
                f"  Stale or insufficient statistics cause over-estimation."
            ),
            recommendation=(
                f"Run: ANALYZE{(' ' + node.relation_name) if node.relation_name else ''};\n"
                "  Consider creating extended statistics for correlated columns: "
                "CREATE STATISTICS <name> ON col1, col2 FROM <table>;"
            ),
            node=node,
            score_impact=8,
        ))


def _detect_buffer_issues(node: PlanNode, findings: List[Finding]) -> None:
    if not node.has_actual:
        return
    total = node.total_buffers
    if total < BUFFER_MIN_BLOCKS:
        return
    ratio = node.buffer_hit_ratio
    if ratio < BUFFER_HIT_RATIO_MIN:
        pct = ratio * 100
        findings.append(Finding(
            level="WARNING",
            category="PLAN",
            title=f"High Disk I/O in '{node.node_type}'"
            + (f" on '{node.relation_name}'" if node.relation_name else ""),
            description=(
                f"Buffer cache hit ratio: {pct:.1f}% "
                f"({node.shared_read_blocks:,} disk reads vs "
                f"{node.shared_hit_blocks:,} cache hits).\n"
                f"  Data is not in PostgreSQL shared_buffers, causing slow I/O."
            ),
            recommendation=(
                "1. Increase shared_buffers (e.g., to 25% of RAM).\n"
                "2. Consider increasing effective_cache_size for better planner hints.\n"
                "3. If this table is frequently accessed, ensure it fits in OS page cache."
            ),
            node=node,
            score_impact=10,
        ))


def _detect_sort_spills(node: PlanNode, findings: List[Finding]) -> None:
    if node.node_type not in ("Sort", "Incremental Sort"):
        return
    if not node.sort_method:
        return
    if "disk" in node.sort_method.lower() or (
        node.sort_space_type and "Disk" in node.sort_space_type
    ):
        findings.append(Finding(
            level="CRITICAL",
            category="PLAN",
            title="Sort Operation Spilling to Disk",
            description=(
                f"Sort method: '{node.sort_method}' | "
                f"Disk space used: {node.sort_space_used:,} kB.\n"
                f"  Sorting on disk is 10-100x slower than in-memory sorting.\n"
                f"  Sort key: {node.sort_key}"
            ),
            recommendation=(
                "Increase work_mem to allow in-memory sorting:\n"
                f"  SET work_mem = '{max(256, node.sort_space_used // 512)}MB';\n"
                "  Or add it to postgresql.conf for a permanent fix.\n"
                "  Alternatively, create an index that pre-sorts the data."
            ),
            node=node,
            score_impact=25,
        ))


def _detect_hash_spills(node: PlanNode, findings: List[Finding]) -> None:
    if node.node_type != "Hash":
        return
    if node.hash_batches > HASH_BATCH_THRESHOLD:
        findings.append(Finding(
            level="WARNING",
            category="PLAN",
            title="Hash Join Spilling to Disk",
            description=(
                f"Hash join required {node.hash_batches} batches "
                f"(original: {node.original_hash_batches}).\n"
                f"  Peak memory usage: {node.peak_memory_usage:,} kB.\n"
                f"  Multi-batch hash joins spill to disk, severely impacting performance."
            ),
            recommendation=(
                f"Increase work_mem to accommodate the hash table in memory:\n"
                f"  SET work_mem = '{max(64, node.peak_memory_usage // 512 * 2)}MB';\n"
                "  Consider rewriting the query to reduce the size of the inner side of the join."
            ),
            node=node,
            score_impact=10,
        ))


def _detect_large_result_no_limit(root: PlanNode, findings: List[Finding]) -> None:
    """Warn if the root returns a huge number of rows with no Limit node."""
    has_limit = any(n.node_type == "Limit" for n in _walk_nodes(root))
    if has_limit:
        return
    rows = root.actual_rows if root.has_actual else root.plan_rows
    if rows >= LARGE_RESULT_ROWS:
        findings.append(Finding(
            level="INFO",
            category="QUERY",
            title="Large Result Set Without LIMIT",
            description=(
                f"Query returns {rows:,} rows with no LIMIT clause.\n"
                "  Fetching large result sets is expensive for both the server and client."
            ),
            recommendation=(
                "Add LIMIT / OFFSET for pagination if not all rows are needed at once.\n"
                "  Consider using cursor-based pagination for large datasets."
            ),
            node=root,
            score_impact=3,
        ))


def _detect_nested_loop_risk(node: PlanNode, findings: List[Finding]) -> None:
    """Detect nested loop joins with large outer row counts."""
    if node.node_type != "Nested Loop":
        return
    if not node.has_actual:
        return
    outer = node.children[0] if node.children else None
    if outer and outer.actual_rows > 10_000:
        findings.append(Finding(
            level="WARNING",
            category="PLAN",
            title="Nested Loop Join with Large Outer Input",
            description=(
                f"Nested loop over {outer.actual_rows:,} outer rows. "
                "Each outer row probes the inner side, causing repeated scans.\n"
                f"  Total actual time: {node.actual_total_time:.3f}ms"
            ),
            recommendation=(
                "Ensure the inner side of the loop has an index. "
                "If the hash or merge join would be better, "
                "try: SET enable_nestloop = off; to compare plans."
            ),
            node=node,
            score_impact=10,
        ))


def _detect_stale_statistics(node: PlanNode, findings: List[Finding]) -> None:
    """Detect when rows_removed_by_filter is very large vs actual_rows."""
    if not node.has_actual or node.rows_removed_by_filter == 0:
        return
    total_processed = node.actual_rows + node.rows_removed_by_filter
    removal_ratio = node.rows_removed_by_filter / total_processed if total_processed > 0 else 0
    if removal_ratio > 0.99 and total_processed > 10_000:
        findings.append(Finding(
            level="WARNING",
            category="STATISTICS",
            title=f"Highly Selective Filter on Seq Scan of '{node.relation_name}'",
            description=(
                f"Filter removed {node.rows_removed_by_filter:,} rows, "
                f"keeping only {node.actual_rows:,} ({(1-removal_ratio)*100:.1f}%).\n"
                f"  Filter: {node.filter}\n"
                "  A {:.1f}% rejection rate on a full scan is extremely inefficient.".format(
                    removal_ratio * 100
                )
            ),
            recommendation=(
                f"Create an index on the filtered column(s): {node.filter}\n"
                "  A partial index may be even more efficient if the filter is always used."
            ),
            node=node,
            score_impact=15,
        ))


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

GRADE_TABLE = [
    (90, "A", "Excellent"),
    (75, "B", "Good"),
    (60, "C", "Fair — improvement recommended"),
    (40, "D", "Poor — significant issues"),
    (0,  "F", "Critical — immediate action required"),
]


def _calculate_score(findings: List[Finding]) -> tuple[int, str]:
    total_deduction = sum(f.score_impact for f in findings)
    score = max(0, min(100, 100 - total_deduction))
    for threshold, grade, _ in GRADE_TABLE:
        if score >= threshold:
            return score, grade
    return score, "F"


# ---------------------------------------------------------------------------
# Main analyzer
# ---------------------------------------------------------------------------

class PlanAnalyzer:
    """Analyzes a PostgreSQL EXPLAIN (ANALYZE, BUFFERS) JSON plan."""

    def analyze_from_json(self, plan_json: dict) -> PlanResult:
        """
        Parse and analyze a plan dict returned by DatabaseConnection.explain_query().
        plan_json is the dict at the top level (contains 'Plan', 'Planning Time', etc.)
        """
        root_node = _parse_node(plan_json.get("Plan", {}))
        planning_time = plan_json.get("Planning Time", 0.0)
        execution_time = plan_json.get("Execution Time", 0.0)

        _annotate_cost_fractions(root_node)
        all_nodes = list(_walk_nodes(root_node))
        has_actual = any(n.has_actual for n in all_nodes)

        findings: List[Finding] = []
        for node in all_nodes:
            _detect_seq_scans(node, findings)
            _detect_estimation_errors(node, findings)
            _detect_buffer_issues(node, findings)
            _detect_sort_spills(node, findings)
            _detect_hash_spills(node, findings)
            _detect_nested_loop_risk(node, findings)
            _detect_stale_statistics(node, findings)

        _detect_large_result_no_limit(root_node, findings)

        score, grade = _calculate_score(findings)

        return PlanResult(
            root_node=root_node,
            planning_time=planning_time,
            execution_time=execution_time,
            findings=findings,
            score=score,
            grade=grade,
            has_actual=has_actual,
            all_nodes=all_nodes,
        )

    def analyze(self, db_conn, query: str, use_analyze: bool = True) -> PlanResult:
        """Full pipeline: run EXPLAIN on the DB and analyze the result."""
        plan_json = db_conn.explain_query(query, use_analyze=use_analyze)
        return self.analyze_from_json(plan_json)


# ---------------------------------------------------------------------------
# Utility helpers used by other modules
# ---------------------------------------------------------------------------

def extract_seq_scan_nodes(plan_result: PlanResult) -> List[PlanNode]:
    return [n for n in plan_result.all_nodes if n.node_type == "Seq Scan" and n.relation_name]


def extract_columns_from_condition(condition: str) -> List[str]:
    """
    Parse a PostgreSQL plan condition string and return bare column names.
    Examples:
      "(amount > 100)"                        -> ["amount"]
      "(o.customer_id = c.id)"               -> ["customer_id", "id"]
      "(status = 'active' AND amount > 0)"   -> ["status", "amount"]
    """
    if not condition:
        return []

    # Remove outer parentheses, string literals, numeric literals
    cleaned = condition.strip("()")
    cleaned = re.sub(r"'[^']*'", " ", cleaned)         # strip string literals
    cleaned = re.sub(r"\b\d+(\.\d+)?\b", " ", cleaned)  # strip numbers
    cleaned = re.sub(r"::[a-zA-Z_]+(\[\])?", " ", cleaned)  # strip type casts

    # SQL keywords / operators to exclude
    EXCLUDE = {
        "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "IS", "IN",
        "ANY", "ALL", "LIKE", "ILIKE", "BETWEEN", "EXISTS", "CASE",
        "WHEN", "THEN", "ELSE", "END", "SIMILAR", "TO",
    }

    # Extract identifiers, stripping table aliases (word.word -> word)
    results = []
    for match in re.finditer(r"\b(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)\b", cleaned, re.IGNORECASE):
        col = match.group(1)
        if col.upper() not in EXCLUDE and len(col) > 1:
            results.append(col)

    return list(dict.fromkeys(results))  # deduplicate, preserve order
