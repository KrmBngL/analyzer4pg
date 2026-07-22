"""
llm_advisor.py - Optional AI-assisted query repair and rewrite suggestions.

analyzer4pg's rule-based engine (query_advisor.py) can only rewrite a fixed
set of known anti-patterns, and it cannot do anything with a query that has
an actual PostgreSQL syntax error (SQLSTATE 42601) -- EXPLAIN just fails.
This module calls the Claude API to go beyond both limits:

  - fix_syntax_error(): repair a query that PostgreSQL refused to parse.
  - suggest_rewrite(): propose a different query structure (e.g. a
    correlated subquery rewritten as a CTE + JOIN) for patterns the
    rule-based rewriter doesn't cover.

Every suggestion from this module is a *proposal only*. The caller (cli.py /
web/app.py) is responsible for re-running it through EXPLAIN (see
rewrite_verify.py) before ever showing it as a fix -- an LLM confirming a
query "looks right" is not evidence it runs, still less that it's faster.

Important limit this module does NOT cover: EXPLAIN verifies a rewrite is
valid SQL and measures its real cost/time on this database. It does NOT
verify the rewrite returns the same result set as the original query. That
semantic-equivalence check is something only the caller can decide to trust
(or verify by actually comparing result sets) -- callers must present AI
suggestions with that caveat.

Requires the `anthropic` package and an ANTHROPIC_API_KEY (or
ANTHROPIC_AUTH_TOKEN) environment variable. Both are optional -- if
unavailable, unavailable_reason() explains why so callers can skip the
feature instead of crashing.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

DEFAULT_MODEL = "claude-opus-4-8"

_SUGGESTION_SCHEMA = {
    "type": "object",
    "properties": {
        "corrected_sql": {
            "type": "string",
            "description": "The corrected/rewritten SQL query, ready to run as-is. Empty string if changed is false.",
        },
        "explanation": {
            "type": "string",
            "description": "One or two sentences in Turkish explaining what changed and why.",
        },
        "changed": {
            "type": "boolean",
            "description": "false if no reliable fix or improvement could be produced.",
        },
    },
    "required": ["corrected_sql", "explanation", "changed"],
    "additionalProperties": False,
}


@dataclass
class LLMSuggestion:
    corrected_sql: str
    explanation: str


class LLMAdvisor:
    """Thin wrapper around the Claude API for query repair/rewrite suggestions."""

    def __init__(self, model: str = DEFAULT_MODEL):
        self.model = model
        self._client = None

    def unavailable_reason(self) -> Optional[str]:
        """None if ready to use; otherwise a Turkish message explaining why not."""
        try:
            import anthropic  # noqa: F401
        except ImportError:
            return "anthropic paketi kurulu değil. Kurulum: pip install anthropic"
        if not (os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN")):
            return "ANTHROPIC_API_KEY tanımlı değil. AI destekli düzeltme için bu ortam değişkenini ayarlayın."
        return None

    def _get_client(self):
        if self._client is None:
            import anthropic
            self._client = anthropic.Anthropic()
        return self._client

    def fix_syntax_error(self, sql: str, error_message: str, server_version: str = "") -> Optional[LLMSuggestion]:
        """Ask Claude to fix a real PostgreSQL syntax error. Returns None if it declines."""
        system = (
            "Sen kıdemli bir PostgreSQL DBA'sısın. Sana sözdizimi hatası veren bir SQL "
            "sorgusu ve PostgreSQL'in verdiği tam hata mesajı verilecek. Görevin: sorgunun "
            "SÖZDİZİMİNİ düzeltmek, orijinal sorgunun amacını (hangi tabloları, sütunları, "
            "koşulları kullandığını) DEĞİŞTİRMEDEN. Var olmayan tablo veya sütun adı uydurma; "
            "sadece sorguda zaten geçen isimleri kullan. Sadece geçerli PostgreSQL sözdizimi "
            "üret. Eğer hatayı güvenilir şekilde düzeltemiyorsan changed=false döndür ve "
            "corrected_sql'i boş bırak."
        )
        user = (
            f"PostgreSQL sürümü: {server_version or 'bilinmiyor'}\n\n"
            f"Sorgu:\n{sql}\n\n"
            f"PostgreSQL hatası:\n{error_message}"
        )
        return self._ask(system, user)

    def suggest_rewrite(
        self, sql: str, plan_summary: str, findings_summary: str, server_version: str = ""
    ) -> Optional[LLMSuggestion]:
        """Ask Claude for an alternative query structure (e.g. subquery -> CTE)."""
        system = (
            "Sen kıdemli bir PostgreSQL performans danışmanısın. Sana çalışan bir SQL "
            "sorgusu, EXPLAIN ANALYZE planının özeti ve tespit edilen performans sorunları "
            "verilecek. Görevin: AYNI sonuç kümesini döndüren, farklı bir sorgu yapısı "
            "önermek (örneğin correlated subquery'yi CTE + JOIN'e çevirmek, gereksiz iç içe "
            "view'ları düzleştirmek, tekrarlanan alt sorguları birleştirmek). Sorgunun "
            "DÖNDÜRDÜĞÜ SONUÇ KÜMESİNİ KESİNLİKLE DEĞİŞTİRME — sadece yapıyı değiştir. Emin "
            "değilsen, daha iyi bir yapı bulamadıysan veya sorgu zaten optimalse changed=false "
            "döndür."
        )
        user = (
            f"PostgreSQL sürümü: {server_version or 'bilinmiyor'}\n\n"
            f"Sorgu:\n{sql}\n\n"
            f"Plan özeti:\n{plan_summary}\n\n"
            f"Tespit edilen sorunlar:\n{findings_summary}"
        )
        return self._ask(system, user)

    def _ask(self, system: str, user: str) -> Optional[LLMSuggestion]:
        reason = self.unavailable_reason()
        if reason:
            raise RuntimeError(reason)

        client = self._get_client()
        try:
            response = client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=system,
                thinking={"type": "adaptive"},
                output_config={"format": {"type": "json_schema", "schema": _SUGGESTION_SCHEMA}},
                messages=[{"role": "user", "content": user}],
            )
        except Exception as e:
            raise RuntimeError(f"Claude API hatası: {e}") from e

        if response.stop_reason == "refusal":
            return None

        text = next((b.text for b in response.content if b.type == "text"), None)
        if not text:
            return None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None

        if not data.get("changed") or not data.get("corrected_sql", "").strip():
            return None

        return LLMSuggestion(
            corrected_sql=data["corrected_sql"].strip(),
            explanation=data.get("explanation", "").strip(),
        )
