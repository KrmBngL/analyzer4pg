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
# Individual detectors
# ---------------------------------------------------------------------------

def _check_select_star(sql: str, normalised: str) -> List[QueryRecommendation]:
    recs = []
    if re.search(r"\bSELECT\s+(\w+\.)?\*", normalised):
        # Extract table name from the actual query for a targeted suggestion
        m = re.search(r"\bFROM\s+(\w+)", sql, re.IGNORECASE)
        tablo = m.group(1) if m else "tablo_adi"
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
            example_before=_extract_fragment(sql, r"\bWHERE\b.{0,120}"),
            example_after="-- Sayısal sütunlar için sayısal literal kullanın:\nWHERE musteri_id = 12345   -- tırnak işareti olmadan",
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
            example_before=_extract_fragment(sql, rf"\b{col}\s*=\s*(?:'[^']*'|\d+)\s+OR\s+{col}\s*=.{{0,80}}"),
            example_after=f"WHERE {col} IN (deger1, deger2, deger3)",
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
                    example_before=_extract_fragment(sql, r"\bHAVING\b.{0,150}"),
                    example_after=(
                        "-- HAVING içindeki aggregate'siz koşulu WHERE'e taşıyın:\n"
                        "SELECT sutun, COUNT(*) FROM tablo\n"
                        "WHERE kosul = 'deger'   -- gruplama öncesi filtre\n"
                        "GROUP BY sutun;"
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
            example_before=_extract_fragment(sql, r"\bUNION\b(?!\s+ALL).{0,200}"),
            example_after=(
                "-- Tekrarlar mümkün değilse UNION ALL kullanın:\n"
                + re.sub(r"\bUNION\b(?!\s+ALL)", "UNION ALL", sql, flags=re.IGNORECASE)[:300]
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
            example_before=_snippet(sql),
            example_after=(
                "-- SELECT listesindeki alt sorguyu JOIN'e dönüştürün:\n"
                "SELECT t1.id, t2.sutun AS etiket\n"
                "FROM ana_tablo t1\n"
                "JOIN alt_tablo t2 ON t2.id = t1.alt_id;"
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
            example_before=_extract_fragment(sql, r"\bCOUNT\s*\([^)]+\)"),
            example_after=(
                "SELECT COUNT(*) FROM tablo;                    -- toplam satır\n"
                "SELECT COUNT(sutun) FROM tablo;                -- NULL olmayanlar\n"
                "SELECT COUNT(*) - COUNT(sutun) FROM tablo;     -- NULL olanlar"
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
