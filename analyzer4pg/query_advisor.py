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


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _normalise(sql: str) -> str:
    """Upper-case, collapse whitespace for regex matching."""
    return re.sub(r"\s+", " ", sql.upper().strip())


def _strip_comments(sql: str) -> str:
    return sqlparse.format(sql, strip_comments=True).strip()


# ---------------------------------------------------------------------------
# Individual detectors
# ---------------------------------------------------------------------------

def _check_select_star(sql: str, normalised: str) -> List[QueryRecommendation]:
    recs = []
    # Match SELECT * or SELECT t.*
    if re.search(r"\bSELECT\s+(\w+\.)?\*", normalised):
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
            example_before="SELECT * FROM orders WHERE status = 'active'",
            example_after="SELECT order_id, customer_id, amount, status FROM orders WHERE status = 'active'",
            score_impact=3,
        ))
    return recs


def _check_leading_wildcard_like(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before="SELECT * FROM products WHERE name LIKE '%widget%'",
            example_after=(
                "-- Seçenek 1: pg_trgm + GIN index\n"
                "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n"
                "CREATE INDEX idx_products_name_trgm ON products USING gin(name gin_trgm_ops);\n"
                "SELECT * FROM products WHERE name LIKE '%widget%';\n\n"
                "-- Seçenek 2: Full-text search\n"
                "SELECT * FROM products WHERE to_tsvector('turkish', name) @@ to_tsquery('widget');"
            ),
            score_impact=10,
        ))
    return recs


def _check_function_on_column(sql: str, normalised: str) -> List[QueryRecommendation]:
    recs = []
    # WHERE func(col) = value  ->  non-SARGable
    # Common patterns: UPPER(), LOWER(), TRIM(), TO_CHAR(), DATE(), EXTRACT()
    patterns = [
        (r"\bWHERE\b.*\b(UPPER|LOWER|TRIM|LTRIM|RTRIM)\s*\(\s*\w+\s*\)\s*(?:=|LIKE|IN)",
         "UPPER/LOWER/TRIM",
         "Fonksiyon içeren koşullarda index kullanılamaz.",
         "WHERE UPPER(email) = 'USER@EXAMPLE.COM'",
         "WHERE email = 'user@example.com'  -- Verileri küçük harfle sakla\n"
         "-- ya da fonksiyonel index: CREATE INDEX idx_email_lower ON users(LOWER(email));"),

        (r"\bWHERE\b.*\b(TO_CHAR|TO_DATE|TO_TIMESTAMP|DATE_TRUNC)\s*\(",
         "Tarih Fonksiyonu",
         "Tarih fonksiyonları koşulda kullanıldığında index atlanır.",
         "WHERE TO_CHAR(created_at, 'YYYY-MM') = '2024-01'",
         "WHERE created_at >= '2024-01-01' AND created_at < '2024-02-01'"),

        (r"\bWHERE\b.*\bEXTRACT\s*\(",
         "EXTRACT Fonksiyonu",
         "EXTRACT() ile karşılaştırma index'i devre dışı bırakır.",
         "WHERE EXTRACT(YEAR FROM order_date) = 2024",
         "WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'"),
    ]

    for pattern, name, desc, before, after in patterns:
        if re.search(pattern, normalised):
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
            ))
    return recs


def _check_not_in_subquery(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before=(
                "SELECT * FROM orders\n"
                "WHERE customer_id NOT IN (SELECT id FROM blacklisted_customers);"
            ),
            example_after=(
                "-- NOT EXISTS kullanımı (NULL-safe ve genellikle daha hızlı)\n"
                "SELECT o.* FROM orders o\n"
                "WHERE NOT EXISTS (\n"
                "    SELECT 1 FROM blacklisted_customers b\n"
                "    WHERE b.id = o.customer_id\n"
                ");\n\n"
                "-- ya da LEFT JOIN / IS NULL\n"
                "SELECT o.* FROM orders o\n"
                "LEFT JOIN blacklisted_customers b ON b.id = o.customer_id\n"
                "WHERE b.id IS NULL;"
            ),
            score_impact=8,
        ))
    return recs


def _check_implicit_type_cast(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before="WHERE customer_id = '12345'   -- customer_id INTEGER iken",
            example_after="WHERE customer_id = 12345   -- doğru tip kullanımı",
            score_impact=5,
        ))
    return recs


def _check_or_instead_of_in(sql: str, normalised: str) -> List[QueryRecommendation]:
    recs = []
    # col = X OR col = Y OR col = Z  ->  col IN (X, Y, Z)
    match = re.search(
        r"\b(\w+)\s*=\s*(?:'[^']*'|\d+)\s+OR\s+\1\s*=\s*(?:'[^']*'|\d+)\s+OR\s+\1\s*=",
        normalised,
    )
    if match:
        col = match.group(1)
        recs.append(QueryRecommendation(
            priority="LOW",
            category="STYLE",
            title="Çok Sayıda OR Koşulu — IN() Kullanın",
            description=(
                f"Aynı sütun '{col}' için tekrarlı OR koşulları yerine IN() daha temiz "
                "ve bazı durumlarda daha verimlidir."
            ),
            example_before=f"WHERE {col} = 1 OR {col} = 2 OR {col} = 3",
            example_after=f"WHERE {col} IN (1, 2, 3)",
            score_impact=2,
        ))
    return recs


def _check_having_vs_where(sql: str, normalised: str) -> List[QueryRecommendation]:
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
                    example_before=(
                        "SELECT department, COUNT(*) FROM employees\n"
                        "GROUP BY department\n"
                        "HAVING department = 'Engineering';"
                    ),
                    example_after=(
                        "SELECT department, COUNT(*) FROM employees\n"
                        "WHERE department = 'Engineering'\n"
                        "GROUP BY department;"
                    ),
                    score_impact=5,
                ))
    return recs


def _check_distinct_abuse(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before="SELECT DISTINCT customer_id FROM orders;",
            example_after=(
                "-- JOIN'i düzeltin veya GROUP BY kullanın\n"
                "SELECT customer_id FROM orders GROUP BY customer_id;\n\n"
                "-- Sadece var mı diye kontrol için\n"
                "SELECT id FROM customers c WHERE EXISTS (\n"
                "    SELECT 1 FROM orders o WHERE o.customer_id = c.id\n"
                ");"
            ),
            score_impact=3,
        ))
    return recs


def _check_union_vs_union_all(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before=(
                "SELECT id FROM current_orders\n"
                "UNION\n"
                "SELECT id FROM archived_orders;"
            ),
            example_after=(
                "-- Tekrarlar mümkün değilse (farklı tablolar, farklı tarih aralıkları vb.)\n"
                "SELECT id FROM current_orders\n"
                "UNION ALL\n"
                "SELECT id FROM archived_orders;"
            ),
            score_impact=5,
        ))
    return recs


def _check_correlated_subquery(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before=(
                "SELECT o.id,\n"
                "       (SELECT c.name FROM customers c WHERE c.id = o.customer_id) AS customer_name\n"
                "FROM orders o;"
            ),
            example_after=(
                "-- JOIN ile tek geçişte çözün\n"
                "SELECT o.id, c.name AS customer_name\n"
                "FROM orders o\n"
                "JOIN customers c ON c.id = o.customer_id;"
            ),
            score_impact=10,
        ))
    return recs


def _check_count_column(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before="SELECT COUNT(manager_id) FROM employees;  -- NULL manager_id'ler sayılmaz",
            example_after=(
                "SELECT COUNT(*) FROM employees;           -- toplam satır\n"
                "SELECT COUNT(manager_id) FROM employees;  -- manager_id olan çalışanlar\n"
                "SELECT COUNT(*) - COUNT(manager_id) FROM employees;  -- manager_id NULL olanlar"
            ),
            score_impact=2,
        ))
    return recs


def _check_offset_large(sql: str, normalised: str) -> List[QueryRecommendation]:
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
                example_before=(
                    "-- Sayfa 1000, sayfa başına 20 satır\n"
                    "SELECT * FROM orders ORDER BY id LIMIT 20 OFFSET 20000;"
                ),
                example_after=(
                    "-- Keyset pagination: son görülen ID'yi referans al\n"
                    "SELECT * FROM orders\n"
                    "WHERE id > :last_seen_id\n"
                    "ORDER BY id\n"
                    "LIMIT 20;"
                ),
                score_impact=5,
            ))
    return recs


def _check_order_by_rand(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before="SELECT * FROM products ORDER BY RANDOM() LIMIT 1;",
            example_after=(
                "-- Tabloya göre daha verimli alternatifler:\n\n"
                "-- Seçenek 1: Yaklaşık rastgele (hızlı, tabmsample)\n"
                "SELECT * FROM products TABLESAMPLE BERNOULLI(1) LIMIT 1;\n\n"
                "-- Seçenek 2: ID aralığında rastgele\n"
                "SELECT * FROM products\n"
                "WHERE id >= (SELECT (MAX(id) - MIN(id)) * RANDOM() + MIN(id) FROM products)\n"
                "ORDER BY id\n"
                "LIMIT 1;"
            ),
            score_impact=10,
        ))
    return recs


def _check_missing_join_condition(sql: str, normalised: str) -> List[QueryRecommendation]:
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
                example_before=(
                    "SELECT o.id, c.name\n"
                    "FROM orders o, customers c\n"
                    "WHERE o.customer_id = c.id;"
                ),
                example_after=(
                    "SELECT o.id, c.name\n"
                    "FROM orders o\n"
                    "JOIN customers c ON c.id = o.customer_id;"
                ),
                score_impact=5,
            ))
    return recs


def _check_unnecessary_subquery(sql: str, normalised: str) -> List[QueryRecommendation]:
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
            example_before=(
                "SELECT sub.id, sub.name\n"
                "FROM (\n"
                "    SELECT id, name FROM customers WHERE status = 'active'\n"
                ") sub\n"
                "WHERE sub.name LIKE 'A%';"
            ),
            example_after=(
                "SELECT id, name\n"
                "FROM customers\n"
                "WHERE status = 'active'\n"
                "  AND name LIKE 'A%';"
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

    def advise(self, sql: str) -> List[QueryRecommendation]:
        """Run all detectors against the given SQL string."""
        cleaned = _strip_comments(sql)
        normalised = _normalise(cleaned)

        recommendations: List[QueryRecommendation] = []
        for detector in self._DETECTORS:
            try:
                recommendations.extend(detector(cleaned, normalised))
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
