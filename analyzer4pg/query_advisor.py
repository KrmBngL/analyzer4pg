"""
query_advisor.py - SQL anti-pattern detection and rewrite suggestions for analyzer4pg
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, Function
from sqlparse.tokens import Keyword, DML, Wildcard, Punctuation


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class QueryRecommendation:
    """A single query rewrite or improvement suggestion."""
    priority: str       # HIGH | MEDIUM | LOW
    category: str       # ANTIPATTERN | REWRITE | STYLE
    title: str
    description: str
    example_before: Optional[str]
    example_after: Optional[str]
    score_impact: int
    rewritten_sql: Optional[str] = None  # full corrected query, only when a safe mechanical rewrite exists


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _normalise(sql: str) -> str:
    """Upper-case, collapse whitespace for regex matching."""
    return re.sub(r"\s+", " ", sql.upper().strip())


def _strip_comments(sql: str) -> str:
    return sqlparse.format(sql, strip_comments=True).strip()


def _snippet(sql: str, max_len: int = 350) -> str:
    """Return the actual SQL (trimmed) as the 'before' example — never mock data."""
    s = sql.strip()
    if len(s) > max_len:
        return s[:max_len].rstrip() + "\n-- ... (sorgu kısaltıldı)"
    return s


def _extract_fragment(sql: str, pattern: str) -> Optional[str]:
    """
    Try to extract the specific fragment of the actual SQL that matches the
    anti-pattern (e.g. the LIKE clause, the function call, etc.).
    Falls back to the full snippet if not found.
    """
    m = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
    if m:
        frag = m.group(0).strip()
        return frag if len(frag) <= 300 else _snippet(sql)
    return _snippet(sql)


# ---------------------------------------------------------------------------
# Mechanical full-query rewrites
#
# These operate on the actual (non-normalised) SQL text and return a
# complete, corrected query — or None if a safe, unambiguous rewrite isn't
# possible. Only patterns where the fix is a pure, meaning-preserving text
# substitution are handled here; anti-patterns that require architectural
# decisions (index choice, join key, pagination strategy, ...) are left to
# the guidance text instead of a fabricated "corrected" query.
# ---------------------------------------------------------------------------

def _rewrite_select_star(sql: str, table_ref: str, db_conn) -> Optional[str]:
    if db_conn is None:
        return None
    try:
        columns = db_conn.fetch_columns(table_ref)
    except Exception:
        return None
    if not columns:
        return None
    m = re.search(r"\bSELECT\s+(\w+\.)?\*", sql, re.IGNORECASE)
    if not m:
        return None
    prefix = m.group(1) or ""
    col_list = ", ".join(f"{prefix}{c}" for c in columns)
    return sql[:m.start()] + f"SELECT {col_list}" + sql[m.end():]


def _rewrite_implicit_cast(sql: str) -> Optional[str]:
    m = re.search(
        r"\b(\w*(?:_id|_code|_num)|status_id|type_id)\s*=\s*'(\d+)'",
        sql,
        re.IGNORECASE,
    )
    if not m:
        return None
    col, val = m.group(1), m.group(2)
    return sql[:m.start()] + f"{col} = {val}" + sql[m.end():]


def _rewrite_or_chain(sql: str, col: str) -> Optional[str]:
    pattern = rf"\b{re.escape(col)}\s*=\s*(?:'[^']*'|\d+)(?:\s+OR\s+{re.escape(col)}\s*=\s*(?:'[^']*'|\d+))+"
    m = re.search(pattern, sql, re.IGNORECASE)
    if not m:
        return None
    values = re.findall(rf"{re.escape(col)}\s*=\s*('[^']*'|\d+)", m.group(0), re.IGNORECASE)
    if len(values) < 3:
        return None
    replacement = f"{col} IN ({', '.join(values)})"
    return sql[:m.start()] + replacement + sql[m.end():]


def _rewrite_union_all(sql: str) -> Optional[str]:
    if not re.search(r"\bUNION\b(?!\s+ALL)", sql, re.IGNORECASE):
        return None
    return re.sub(r"\bUNION\b(?!\s+ALL)", "UNION ALL", sql, flags=re.IGNORECASE)


def _rewrite_upper_lower_eq(sql: str) -> Optional[str]:
    m = re.search(r"\b(?:UPPER|LOWER)\s*\(\s*([\w\.]+)\s*\)\s*=\s*'([^']*)'", sql, re.IGNORECASE)
    if not m:
        return None
    col, val = m.group(1), m.group(2)
    if any(ch in val for ch in "%_"):
        return None  # would change ILIKE wildcard semantics
    return sql[:m.start()] + f"{col} ILIKE '{val}'" + sql[m.end():]


def _rewrite_extract_year(sql: str) -> Optional[str]:
    m = re.search(r"\bEXTRACT\s*\(\s*YEAR\s+FROM\s+([\w\.]+)\s*\)\s*=\s*(\d{4})\b", sql, re.IGNORECASE)
    if not m:
        return None
    col, year = m.group(1), int(m.group(2))
    replacement = f"{col} >= '{year}-01-01' AND {col} < '{year + 1}-01-01'"
    return sql[:m.start()] + replacement + sql[m.end():]


def _rewrite_having_to_where(sql: str) -> Optional[str]:
    m_having = re.search(
        r"\bHAVING\s+(.*?)(?=\bORDER\s+BY\b|\bLIMIT\b|;|$)", sql, re.IGNORECASE | re.DOTALL
    )
    if not m_having:
        return None
    having_cond = m_having.group(1).strip()
    if not having_cond:
        return None
    sql_no_having = (sql[:m_having.start()] + sql[m_having.end():]).rstrip()

    m_group = re.search(r"\bGROUP\s+BY\b", sql_no_having, re.IGNORECASE)
    if not m_group:
        return None
    pre_group = sql_no_having[:m_group.start()]
    has_where = re.search(r"\bWHERE\b", pre_group, re.IGNORECASE)

    if has_where:
        insertion = f"\n  AND ({having_cond})\n"
    else:
        insertion = f"\nWHERE {having_cond}\n"
    return pre_group.rstrip() + insertion + sql_no_having[m_group.start():]


def _find_matching_paren(sql: str, open_idx: int) -> int:
    """Given the index of an opening '(', return the index of its matching ')'."""
    depth = 0
    in_string = False
    i = open_idx
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            if in_string and i + 1 < n and sql[i + 1] == "'":
                i += 2
                continue
            in_string = not in_string
        elif not in_string:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return -1


def _split_top_level(text: str, keyword: str) -> List[str]:
    """Split text on a keyword (e.g. 'AND'), ignoring occurrences inside
    nested parentheses or string literals."""
    parts = []
    depth = 0
    in_string = False
    start = 0
    i = 0
    n = len(text)
    kw_len = len(keyword)
    while i < n:
        ch = text[i]
        if ch == "'":
            if in_string and i + 1 < n and text[i + 1] == "'":
                i += 2
                continue
            in_string = not in_string
        elif not in_string:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0 and text[i:i + kw_len].upper() == keyword.upper():
                before = text[i - 1] if i > 0 else " "
                after = text[i + kw_len] if i + kw_len < n else " "
                if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
                    parts.append(text[start:i])
                    i += kw_len
                    start = i
                    continue
        i += 1
    parts.append(text[start:])
    return [p.strip() for p in parts if p.strip()]


def _rewrite_correlated_subquery(sql: str) -> Optional[str]:
    """
    Converts scalar correlated subqueries in the SELECT list into LEFT JOINs,
    when each one has the simple shape:
        (SELECT <col> FROM <table> <alias> WHERE <filters> AND <alias>.<col> = <outer>.<col>)
    A LEFT JOIN preserves the original NULL-if-no-match semantics exactly.

    When several subqueries hit the *same* lookup table with a single equality
    filter each (the classic EAV/attribute-value pattern), they're folded into
    one shared CTE that pre-filters the lookup table down to the relevant rows
    before joining — this is what keeps the plan cheap on a large lookup table,
    instead of scanning it once per subquery.

    Only the simple shape above is converted; anything more complex (nested
    joins, aggregates, non-equality correlation, ...) is left untouched.
    Returns None if nothing could be safely converted. The caller is expected
    to verify the result is actually cheaper via EXPLAIN before presenting it
    — this function only guarantees a *correct*, not necessarily faster, query.
    """
    m_select = re.match(r"\s*SELECT\b", sql, re.IGNORECASE)
    if not m_select:
        return None

    # Locate the top-level FROM that ends the outer SELECT list (paren-depth 0)
    depth = 0
    in_string = False
    from_idx = -1
    i = m_select.end()
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            if in_string and i + 1 < n and sql[i + 1] == "'":
                i += 2
                continue
            in_string = not in_string
        elif not in_string:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0 and re.match(r"\bFROM\b", sql[i:], re.IGNORECASE):
                from_idx = i
                break
        i += 1
    if from_idx == -1:
        return None

    matches = []
    pos = m_select.end()
    while True:
        open_idx = sql.find("(", pos, from_idx)
        if open_idx == -1:
            break
        if not re.match(r"\(\s*SELECT\b", sql[open_idx:], re.IGNORECASE):
            pos = open_idx + 1
            continue
        close_idx = _find_matching_paren(sql, open_idx)
        if close_idx == -1 or close_idx >= from_idx:
            pos = open_idx + 1
            continue

        inner = sql[open_idx + 1:close_idx]
        m_sub = re.match(
            r"\s*SELECT\s+([\w\.]+)\s+FROM\s+([\w\.]+)\s+(\w+)\s+WHERE\s+(.+)$",
            inner, re.IGNORECASE | re.DOTALL,
        )
        if m_sub:
            select_expr, sub_table, sub_alias, where_cond = m_sub.groups()
            conditions = _split_top_level(where_cond, "AND")
            sub_prefix = sub_alias.lower() + "."

            correlation_col = None
            outer_ref = None
            const_filters = []     # [(col_name, literal), ...] — simple "alias.col = literal" filters
            extra_conditions = []  # anything we can't classify — kept verbatim in the ON clause

            for cond in conditions:
                m_eq = re.match(r"^([\w\.]+)\s*=\s*([\w\.]+)$", cond)
                if correlation_col is None and m_eq:
                    left, right = m_eq.groups()
                    if left.lower().startswith(sub_prefix):
                        correlation_col, outer_ref = left.split(".", 1)[1], right
                        continue
                    if right.lower().startswith(sub_prefix):
                        correlation_col, outer_ref = right.split(".", 1)[1], left
                        continue
                m_lit = re.match(rf"^{re.escape(sub_alias)}\.(\w+)\s*=\s*('[^']*'|\d+)$", cond, re.IGNORECASE)
                if m_lit:
                    const_filters.append((m_lit.group(1), m_lit.group(2)))
                else:
                    extra_conditions.append(cond)

            if correlation_col and outer_ref:
                matches.append({
                    "open": open_idx, "close": close_idx,
                    "select_expr": select_expr, "select_col": select_expr.split(".")[-1],
                    "sub_table": sub_table, "sub_alias": sub_alias,
                    "correlation_col": correlation_col, "outer_ref": outer_ref,
                    "const_filters": const_filters, "extra_conditions": extra_conditions,
                    "all_conditions": conditions,
                })
        pos = close_idx + 1

    if not matches:
        return None

    groups: dict = {}
    for m in matches:
        groups.setdefault(m["sub_table"].lower(), []).append(m)

    cte_defs = []
    joins = []
    cte_seq = 0

    for group in groups.values():
        homogeneous = (
            len(group) > 1
            and all(len(m["const_filters"]) == 1 and not m["extra_conditions"] for m in group)
            and len({m["const_filters"][0][0] for m in group}) == 1
            and len({m["correlation_col"] for m in group}) == 1
            and len({m["select_col"] for m in group}) == 1
        )
        if homogeneous:
            filter_col = group[0]["const_filters"][0][0]
            corr_col = group[0]["correlation_col"]
            select_col = group[0]["select_col"]
            sub_table = group[0]["sub_table"]
            values, seen = [], set()
            for m in group:
                v = m["const_filters"][0][1]
                if v not in seen:
                    seen.add(v)
                    values.append(v)
            cte_seq += 1
            short = re.sub(r"\W+", "_", sub_table.split(".")[-1]).strip("_") or "t"
            cte_name = f"cte_{short}_{cte_seq}"
            cte_defs.append(
                f"{cte_name} AS (\n"
                f"    SELECT {corr_col}, {filter_col}, {select_col}\n"
                f"    FROM {sub_table}\n"
                f"    WHERE {filter_col} IN ({', '.join(values)})\n"
                f")"
            )
            for m in group:
                filt_val = m["const_filters"][0][1]
                joins.append(
                    f"LEFT JOIN {cte_name} {m['sub_alias']} "
                    f"ON {m['sub_alias']}.{corr_col} = {m['outer_ref']} "
                    f"AND {m['sub_alias']}.{filter_col} = {filt_val}"
                )
                m["replacement"] = f"{m['sub_alias']}.{select_col}"
        else:
            for m in group:
                joins.append(f"LEFT JOIN {m['sub_table']} {m['sub_alias']} ON {' AND '.join(m['all_conditions'])}")
                m["replacement"] = m["select_expr"]

    result = sql
    for m in sorted(matches, key=lambda x: x["open"], reverse=True):
        result = result[:m["open"]] + m["replacement"] + result[m["close"] + 1:]

    m_tail = re.search(r"\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b", result, re.IGNORECASE)
    join_block = "\n".join(joins)
    if m_tail:
        result = result[:m_tail.start()] + "\n" + join_block + "\n" + result[m_tail.start():]
    else:
        result = result.rstrip().rstrip(";") + "\n" + join_block + ";"

    if cte_defs:
        result = "WITH " + ",\n".join(cte_defs) + "\n" + result

    return result


# ---------------------------------------------------------------------------
# Individual detectors
# ---------------------------------------------------------------------------

def _check_select_star(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    if re.search(r"\bSELECT\s+(\w+\.)?\*", normalised):
        # Extract table name from the actual query for a targeted suggestion
        m = re.search(r"\bFROM\s+([\w\.]+)", sql, re.IGNORECASE)
        tablo = m.group(1) if m else "tablo_adi"
        rewritten = _rewrite_select_star(sql, tablo, db_conn) if m else None
        recs.append(QueryRecommendation(
            priority="LOW",
            category="ANTIPATTERN",
            title="SELECT * Kullanımı",
            description=(
                "SELECT * tüm sütunları çeker; bu gereksiz veri transferine ve "
                "index-only scan'in kullanılamamasına neden olur.\n"
                "  - Ağ trafiğini artırır.\n"
                "  - İhtiyaç duyulmayan TOAST/büyük sütunları da getirir.\n"
                "  - Sütun sırası değiştiğinde uygulama hatalarına yol açabilir."
            ),
            example_before=_snippet(sql),
            example_after=f"-- SELECT * yerine ihtiyaç duyduğunuz sütunları listeleyin:\nSELECT id, ad, tarih, ... FROM {tablo} WHERE ...;",
            score_impact=3,
            rewritten_sql=rewritten,
        ))
    return recs


def _check_leading_wildcard_like(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # LIKE '%...' or ILIKE '%...'
    if re.search(r"\b(?:I?LIKE)\s+'%\w", normalised):
        recs.append(QueryRecommendation(
            priority="HIGH",
            category="ANTIPATTERN",
            title="Baştaki Wildcard ile LIKE '%...' Kullanımı",
            description=(
                "LIKE '%metin' veya ILIKE '%metin' kalıpları B-tree index'i kullanamaz. "
                "Bu, tüm tabloyu taramaya (Seq Scan) zorlar.\n"
                "  - pg_trgm extension ile GIN index kullanılabilir.\n"
                "  - Tam metin arama için tsvector/tsquery tercih edin."
            ),
            example_before=_extract_fragment(sql, r"\b\w+\s+I?LIKE\s+'%[^']*'"),
            example_after=(
                "-- Seçenek 1: pg_trgm + GIN index (hızlı LIKE '%...%')\n"
                "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n"
                "CREATE INDEX idx_tablo_sutun_trgm ON tablo USING gin(sutun gin_trgm_ops);\n"
                "-- Artık mevcut sorgunuz index kullanır\n\n"
                "-- Seçenek 2: Full-text search\n"
                "WHERE to_tsvector('turkish', sutun) @@ to_tsquery('aranan_kelime');"
            ),
            score_impact=10,
        ))
    return recs


def _check_function_on_column(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # WHERE func(col) = value  ->  non-SARGable
    # Common patterns: UPPER(), LOWER(), TRIM(), TO_CHAR(), DATE(), EXTRACT()
    patterns = [
        (r"\bWHERE\b.*\b(UPPER|LOWER|TRIM|LTRIM|RTRIM)\s*\(\s*\w+\s*\)\s*(?:=|LIKE|IN)",
         "UPPER/LOWER/TRIM",
         "Fonksiyon içeren koşullarda index kullanılamaz.",
         "WHERE UPPER(email) = 'USER@EXAMPLE.COM'",
         "WHERE email = 'user@example.com'  -- Verileri küçük harfle sakla\n"
         "-- ya da fonksiyonel index: CREATE INDEX idx_email_lower ON users(LOWER(email));",
         _rewrite_upper_lower_eq),

        (r"\bWHERE\b.*\b(TO_CHAR|TO_DATE|TO_TIMESTAMP|DATE_TRUNC)\s*\(",
         "Tarih Fonksiyonu",
         "Tarih fonksiyonları koşulda kullanıldığında index atlanır.",
         "WHERE TO_CHAR(created_at, 'YYYY-MM') = '2024-01'",
         "WHERE created_at >= '2024-01-01' AND created_at < '2024-02-01'",
         None),

        (r"\bWHERE\b.*\bEXTRACT\s*\(",
         "EXTRACT Fonksiyonu",
         "EXTRACT() ile karşılaştırma index'i devre dışı bırakır.",
         "WHERE EXTRACT(YEAR FROM order_date) = 2024",
         "WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'",
         _rewrite_extract_year),
    ]

    for pattern, name, desc, before, after, rewrite_fn in patterns:
        if re.search(pattern, normalised):
            rewritten = rewrite_fn(sql) if rewrite_fn else None
            recs.append(QueryRecommendation(
                priority="HIGH",
                category="ANTIPATTERN",
                title=f"WHERE Koşulunda Fonksiyon: {name}",
                description=(
                    f"{desc}\n"
                    "  Index kullanılamadığı için tam tablo taraması (Seq Scan) yapılır.\n"
                    "  SARGable (Search ARGument ABLE) koşullar index'i aktive eder."
                ),
                example_before=before,
                example_after=after,
                score_impact=10,
                rewritten_sql=rewritten,
            ))
    return recs


def _check_not_in_subquery(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    if re.search(r"\bNOT\s+IN\s*\(\s*SELECT\b", normalised):
        recs.append(QueryRecommendation(
            priority="HIGH",
            category="REWRITE",
            title="NOT IN (SELECT ...) — NULL Tuzağı",
            description=(
                "NOT IN ile subquery kullanımı, subquery'de tek bir NULL değeri olduğunda "
                "hiç satır döndürmez (NULL karşılaştırması her zaman UNKNOWN).\n"
                "  Ayrıca büyük subquery sonuçlarında performans sorunlarına yol açar.\n"
                "  NOT EXISTS genellikle daha iyi bir plan seçer."
            ),
            example_before=_extract_fragment(sql, r"\bNOT\s+IN\s*\([\s\S]{0,200}?\)"),
            example_after=(
                "-- NOT EXISTS kullanımı (NULL-safe ve genellikle daha hızlı)\n"
                "SELECT t.* FROM ana_tablo t\n"
                "WHERE NOT EXISTS (\n"
                "    SELECT 1 FROM alt_tablo a\n"
                "    WHERE b.id = o.customer_id\n"
                ");\n\n"
                "-- ya da LEFT JOIN / IS NULL\n"
                "SELECT t.* FROM ana_tablo t\n"
                "LEFT JOIN alt_tablo a ON a.id = t.alt_id\n"
                "WHERE a.id IS NULL;"
            ),
            score_impact=8,
        ))
    return recs


def _check_implicit_type_cast(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # Integer column compared with string literal: col = '123' (common in WHERE clauses)
    # This is heuristic - we look for numeric-named cols compared to string literals
    if re.search(r"\b(_?ID|_?CODE|_?NUM|STATUS_ID|TYPE_ID)\s*=\s*'[^']*'", normalised):
        recs.append(QueryRecommendation(
            priority="MEDIUM",
            category="ANTIPATTERN",
            title="Örtük Tip Dönüşümü (Implicit Cast)",
            description=(
                "Sayısal sütunu string literal ile karşılaştırmak tip dönüşümü gerektirir.\n"
                "  Bu durum index'in kullanılmamasına neden olabilir.\n"
                "  PostgreSQL bazen dönüşümü otomatik yapar ancak plan verimsizleşebilir."
            ),
            example_before=_extract_fragment(sql, r"\bWHERE\b.{0,120}"),
            example_after="-- Sayısal sütunlar için sayısal literal kullanın:\nWHERE musteri_id = 12345   -- tırnak işareti olmadan",
            score_impact=5,
            rewritten_sql=_rewrite_implicit_cast(sql),
        ))
    return recs


def _check_or_instead_of_in(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # col = X OR col = Y OR col = Z  ->  col IN (X, Y, Z)
    match = re.search(
        r"\b(\w+)\s*=\s*(?:'[^']*'|\d+)\s+OR\s+\1\s*=\s*(?:'[^']*'|\d+)\s+OR\s+\1\s*=",
        normalised,
    )
    if match:
        col_upper = match.group(1)
        # Recover the column's original casing from the actual SQL text
        col_match = re.search(rf"\b{re.escape(col_upper)}\b", sql, re.IGNORECASE)
        col = col_match.group(0) if col_match else col_upper
        recs.append(QueryRecommendation(
            priority="LOW",
            category="STYLE",
            title="Çok Sayıda OR Koşulu — IN() Kullanın",
            description=(
                f"Aynı sütun '{col}' için tekrarlı OR koşulları yerine IN() daha temiz "
                "ve bazı durumlarda daha verimlidir."
            ),
            example_before=_extract_fragment(sql, rf"\b{col}\s*=\s*(?:'[^']*'|\d+)\s+OR\s+{col}\s*=.{{0,80}}"),
            example_after=f"WHERE {col} IN (deger1, deger2, deger3)",
            score_impact=2,
            rewritten_sql=_rewrite_or_chain(sql, col),
        ))
    return recs


def _check_having_vs_where(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # HAVING without GROUP BY, or HAVING on a non-aggregate column
    if re.search(r"\bHAVING\b", normalised):
        # Check if HAVING is used without aggregate functions (common mistake)
        having_match = re.search(r"\bHAVING\s+(.+?)(?:\bORDER\b|\bLIMIT\b|$)", normalised)
        if having_match:
            having_clause = having_match.group(1)
            has_aggregate = re.search(
                r"\b(COUNT|SUM|AVG|MAX|MIN|ARRAY_AGG|STRING_AGG)\s*\(", having_clause
            )
            if not has_aggregate:
                recs.append(QueryRecommendation(
                    priority="MEDIUM",
                    category="REWRITE",
                    title="HAVING Koşulu WHERE ile Değiştirilebilir",
                    description=(
                        "HAVING içindeki koşul bir aggregate fonksiyon içermiyor.\n"
                        "  HAVING, GROUP BY sonrası filtreleme yapar — tüm gruplar hesaplandıktan sonra.\n"
                        "  WHERE koşulu gruplama öncesinde çalışır ve çok daha verimlidir."
                    ),
                    example_before=_extract_fragment(sql, r"\bHAVING\b.{0,150}"),
                    example_after=(
                        "-- HAVING içindeki aggregate'siz koşulu WHERE'e taşıyın:\n"
                        "SELECT sutun, COUNT(*) FROM tablo\n"
                        "WHERE kosul = 'deger'   -- gruplama öncesi filtre\n"
                        "GROUP BY sutun;"
                    ),
                    score_impact=5,
                    rewritten_sql=_rewrite_having_to_where(sql),
                ))
    return recs


def _check_distinct_abuse(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    if re.search(r"\bSELECT\s+DISTINCT\b", normalised):
        recs.append(QueryRecommendation(
            priority="LOW",
            category="ANTIPATTERN",
            title="SELECT DISTINCT — Gereksiz Tekrar Silme",
            description=(
                "SELECT DISTINCT pahalı bir sıralama/hash işlemi gerektirir.\n"
                "  Genellikle yanlış veya eksik JOIN koşullarının belirtisidir.\n"
                "  - JOIN'lerinizi kontrol edin: gereksiz çoklama yapıyor mu?\n"
                "  - Sadece benzersiz satırlar gerekiyorsa DISTINCT ON() kullanın.\n"
                "  - Varlık kontrolü için EXISTS daha verimlidir."
            ),
            example_before=_snippet(sql),
            example_after=(
                "-- JOIN'i düzeltin veya GROUP BY kullanın\n"
                "SELECT sutun FROM tablo GROUP BY sutun;\n\n"
                "-- Sadece var mı diye kontrol için\n"
                "SELECT id FROM ana_tablo a WHERE EXISTS (\n"
                "    SELECT 1 FROM alt_tablo b WHERE b.ana_id = a.id\n"
                ");"
            ),
            score_impact=3,
        ))
    return recs


def _check_union_vs_union_all(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # UNION without ALL
    if re.search(r"\bUNION\b(?!\s+ALL)", normalised):
        recs.append(QueryRecommendation(
            priority="MEDIUM",
            category="ANTIPATTERN",
            title="UNION Yerine UNION ALL Kullanımı",
            description=(
                "UNION, yinelenen satırları kaldırmak için ek bir sıralama/hash adımı ekler.\n"
                "  Eğer sonuç setlerinde tekrar olamayacağı biliniyorsa "
                "(örn. farklı tablolar), UNION ALL çok daha hızlıdır."
            ),
            example_before=_extract_fragment(sql, r"\bUNION\b(?!\s+ALL).{0,200}"),
            example_after="-- Tekrarlar mümkün değilse UNION ALL kullanın:\nUNION ALL",
            score_impact=5,
            rewritten_sql=_rewrite_union_all(sql),
        ))
    return recs


def _check_correlated_subquery(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # SELECT (SELECT ... FROM t2 WHERE t2.col = t1.col) FROM t1
    # Heuristic: subquery in SELECT list
    if re.search(r"\bSELECT\b[^;]+\(\s*SELECT\b[^)]+\bWHERE\b[^)]+\.[^)]+=[^)]+\.", normalised):
        recs.append(QueryRecommendation(
            priority="HIGH",
            category="REWRITE",
            title="SELECT Listesinde İlişkisel Alt Sorgu (Correlated Subquery)",
            description=(
                "SELECT listesindeki ilişkisel alt sorgular her satır için ayrı ayrı çalışır.\n"
                "  N satır için N kez sorgu = N+1 problemi.\n"
                "  Bu, büyük tablolarda ciddi performans sorununa yol açar."
            ),
            example_before=_snippet(sql),
            example_after=(
                "-- SELECT listesindeki alt sorguyu JOIN'e dönüştürün:\n"
                "SELECT t1.id, t2.sutun AS etiket\n"
                "FROM ana_tablo t1\n"
                "JOIN alt_tablo t2 ON t2.id = t1.alt_id;"
            ),
            score_impact=10,
            rewritten_sql=_rewrite_correlated_subquery(sql),
        ))
    return recs


def _check_count_column(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # COUNT(column) vs COUNT(*) - often misunderstood
    if re.search(r"\bCOUNT\s*\(\s*(?![\*1])\w+\s*\)", normalised):
        recs.append(QueryRecommendation(
            priority="LOW",
            category="STYLE",
            title="COUNT(sütun) — NULL Davranışını Bilin",
            description=(
                "COUNT(sütun), NULL değerleri saymaz; COUNT(*) veya COUNT(1) tüm satırları sayar.\n"
                "  Bu fark, yanlış sonuçlara yol açabilir.\n"
                "  - Satır sayısı için: COUNT(*) veya COUNT(1)\n"
                "  - NULL olmayan değer sayısı için: COUNT(sütun)\n"
                "  - Benzersiz değer sayısı için: COUNT(DISTINCT sütun)"
            ),
            example_before=_extract_fragment(sql, r"\bCOUNT\s*\([^)]+\)"),
            example_after=(
                "SELECT COUNT(*) FROM tablo;                    -- toplam satır\n"
                "SELECT COUNT(sutun) FROM tablo;                -- NULL olmayanlar\n"
                "SELECT COUNT(*) - COUNT(sutun) FROM tablo;     -- NULL olanlar"
            ),
            score_impact=2,
        ))
    return recs


def _check_offset_large(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    match = re.search(r"\bOFFSET\s+(\d+)\b", normalised)
    if match:
        offset_val = int(match.group(1))
        if offset_val > 10_000:
            recs.append(QueryRecommendation(
                priority="MEDIUM",
                category="ANTIPATTERN",
                title=f"Büyük OFFSET Değeri: {offset_val:,}",
                description=(
                    f"OFFSET {offset_val:,} kullanımı, veritabanının önce {offset_val:,} satırı "
                    "işleyip atmasını gerektirir.\n"
                    "  Sayfa numarası büyüdükçe sorgu giderek yavaşlar.\n"
                    "  'Keyset pagination' (cursor-based) çok daha verimlidir."
                ),
                example_before=_snippet(sql),
                example_after=(
                    "-- Keyset pagination ile değiştirin:\n"
                    "SELECT ... FROM tablo\n"
                    "WHERE id > :son_gorülen_id   -- önceki sayfanın son ID'si\n"
                    "ORDER BY id\n"
                    "LIMIT :sayfa_boyutu;"
                ),
                score_impact=5,
            ))
    return recs


def _check_order_by_rand(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    if re.search(r"\bORDER\s+BY\s+RANDOM\s*\(\)", normalised):
        recs.append(QueryRecommendation(
            priority="HIGH",
            category="ANTIPATTERN",
            title="ORDER BY RANDOM() — Çok Pahalı Rastgele Sıralama",
            description=(
                "ORDER BY RANDOM() tüm tabloyu bellekte sıralar, sonra bir satır seçer.\n"
                "  Büyük tablolarda çok yavaştır (O(N log N) kompleksite)."
            ),
            example_before=_extract_fragment(sql, r"\bORDER\s+BY\s+RANDOM\s*\(\).*"),
            example_after=(
                "-- Tabloya göre daha verimli alternatifler:\n\n"
                "-- Seçenek 1: Yaklaşık rastgele (hızlı, tabmsample)\n"
                "SELECT * FROM tablo TABLESAMPLE BERNOULLI(1) LIMIT 1;\n\n"
                "-- Seçenek 2: ID aralığında rastgele\n"
                "SELECT * FROM tablo\n"
                "WHERE id >= (SELECT (MAX(id) - MIN(id)) * RANDOM() + MIN(id) FROM tablo)\n"
                "ORDER BY id\n"
                "LIMIT 1;"
            ),
            score_impact=10,
        ))
    return recs


def _check_missing_join_condition(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # Detect implicit cross join: FROM t1, t2 without a WHERE join condition
    # Simple heuristic: multiple tables in FROM without JOIN keyword
    from_match = re.search(r"\bFROM\s+([\w\s,]+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|;|$)", normalised)
    if from_match:
        from_clause = from_match.group(1)
        tables = [t.strip() for t in from_clause.split(",") if t.strip()]
        if len(tables) > 1:
            recs.append(QueryRecommendation(
                priority="HIGH",
                category="ANTIPATTERN",
                title="Virgülle Ayrılmış Tablolar — Olası Kartezyen Çarpım",
                description=(
                    "FROM t1, t2 sözdizimi implicit cross join oluşturur.\n"
                    "  WHERE'de join koşulu yoksa kartezyen çarpım (N×M satır) üretilir.\n"
                    "  Modern SQL'de explicit JOIN sözdizimi kullanın."
                ),
                example_before=_snippet(sql),
                example_after=(
                    "-- Explicit JOIN sözdizimini kullanın:\n"
                    "SELECT t1.sutun, t2.sutun\n"
                    "FROM tablo1 t1\n"
                    "JOIN tablo2 t2 ON t2.id = t1.tablo2_id;"
                ),
                score_impact=5,
            ))
    return recs


def _check_unnecessary_subquery(sql: str, normalised: str, db_conn=None) -> List[QueryRecommendation]:
    recs = []
    # SELECT ... FROM (SELECT ... FROM t) sub  where inner has no GROUP BY/DISTINCT
    if re.search(r"\bFROM\s*\(\s*SELECT\b(?:(?!\bGROUP\b|\bDISTINCT\b|\bLIMIT\b|\bUNION\b).)*\)\s+\w+\b", normalised):
        recs.append(QueryRecommendation(
            priority="LOW",
            category="REWRITE",
            title="Gereksiz Alt Sorgu (Inline View)",
            description=(
                "GROUP BY / DISTINCT / LIMIT içermeyen iç sorgular genellikle gereksizdir.\n"
                "  PostgreSQL bu durumda 'subquery flattening' yapabilir, ama her zaman değil.\n"
                "  Doğrudan dış sorguda JOIN veya CTE kullanmak daha temiz ve bazen daha hızlıdır."
            ),
            example_before=_snippet(sql),
            example_after=(
                "-- İç sorguyu düzleştirin, koşulları WHERE'e taşıyın:\n"
                "SELECT sutun1, sutun2\n"
                "FROM tablo\n"
                "WHERE kosul1 = 'deger'\n"
                "  AND kosul2 LIKE 'A%';"
            ),
            score_impact=2,
        ))
    return recs


# ---------------------------------------------------------------------------
# Main advisor
# ---------------------------------------------------------------------------

class QueryAdvisor:
    """Detects SQL anti-patterns and generates rewrite recommendations."""

    _DETECTORS = [
        _check_select_star,
        _check_leading_wildcard_like,
        _check_function_on_column,
        _check_not_in_subquery,
        _check_implicit_type_cast,
        _check_or_instead_of_in,
        _check_having_vs_where,
        _check_distinct_abuse,
        _check_union_vs_union_all,
        _check_correlated_subquery,
        _check_count_column,
        _check_offset_large,
        _check_order_by_rand,
        _check_missing_join_condition,
        _check_unnecessary_subquery,
    ]

    def advise(self, sql: str, db_conn=None) -> List[QueryRecommendation]:
        """
        Run all detectors against the given SQL string.
        db_conn (optional): an open DatabaseConnection, used only to resolve
        real column names for the SELECT * rewrite suggestion.
        """
        cleaned = _strip_comments(sql)
        normalised = _normalise(cleaned)

        recommendations: List[QueryRecommendation] = []
        for detector in self._DETECTORS:
            try:
                recommendations.extend(detector(cleaned, normalised, db_conn))
            except Exception:
                pass  # Never crash the full analysis due to a single detector

        # Sort by priority
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        recommendations.sort(key=lambda r: order.get(r.priority, 3))
        return recommendations


def format_sql(sql: str) -> str:
    """Return a prettified version of the SQL."""
    try:
        return sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            identifier_case="lower",
            strip_comments=False,
            indent_width=4,
        )
    except Exception:
        return sql
