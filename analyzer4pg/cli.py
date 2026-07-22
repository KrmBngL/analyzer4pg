"""
cli.py - Command-line interface and interactive REPL for analyzer4pg
"""

from __future__ import annotations

import os
import sys
import getpass
from typing import Optional

import click
from rich.prompt import Prompt
from rich import box
from rich.panel import Panel

from . import __version__
from .connection import DatabaseConnection, build_connection_config, QuerySyntaxError
from .plan_analyzer import PlanAnalyzer
from .index_advisor import IndexAdvisor
from .query_advisor import QueryAdvisor
from .llm_advisor import LLMAdvisor
from .rewrite_verify import verify_rewrite
from .reporter import (
    console,
    print_full_report,
    print_header,
)

# ---------------------------------------------------------------------------
# Shared connection options
# ---------------------------------------------------------------------------

_connection_options = [
    click.option("-H", "--host",     default="localhost", show_default=True, help="PostgreSQL sunucu adresi"),
    click.option("-p", "--port",     default=5432,        show_default=True, type=int, help="Port numarası"),
    click.option("-d", "--dbname",   default="postgres",  show_default=True, help="Veritabanı adı"),
    click.option("-U", "--user",     default="postgres",  show_default=True, help="Kullanıcı adı"),
    click.option("-W", "--password", default=None,        help="Parola (belirtilmezse sorulur)"),
    click.option("--sslmode",        default="prefer",    show_default=True,
                 type=click.Choice(["disable","allow","prefer","require","verify-ca","verify-full"]),
                 help="SSL modu"),
    click.option("--no-analyze",     is_flag=True,        default=False,
                 help="EXPLAIN ANALYZE yerine sadece EXPLAIN çalıştır (sorguyu gerçekten çalıştırmaz)"),
    click.option("--ai",             "use_ai", is_flag=True, default=False,
                 help="AI destekli sözdizimi düzeltme ve sorgu önerisi (ANTHROPIC_API_KEY gerekir)"),
]


def add_connection_options(func):
    for option in reversed(_connection_options):
        func = option(func)
    return func


def _get_password(host: str, port: int, dbname: str, user: str, password: Optional[str]) -> Optional[str]:
    """Resolve password: CLI flag → PGPASSWORD env → interactive prompt."""
    if password:
        return password
    env_pass = os.environ.get("PGPASSWORD")
    if env_pass:
        return env_pass
    # Don't prompt if stdout is not a TTY (e.g. piped input)
    if sys.stdin.isatty():
        try:
            return getpass.getpass(f"Parola ({user}@{host}:{port}/{dbname}): ")
        except (EOFError, KeyboardInterrupt):
            return None
    return None


def _connect(host, port, dbname, user, password, sslmode) -> DatabaseConnection:
    password = _get_password(host, port, dbname, user, password)
    cfg = build_connection_config(host, port, dbname, user, password, sslmode)
    db = DatabaseConnection(cfg)
    db.connect()
    return db


def _run_analysis(
    db: DatabaseConnection,
    sql: str,
    use_analyze: bool,
    show_sql: bool = True,
    use_ai: bool = False,
) -> None:
    """Core pipeline: EXPLAIN → analyze plan → index advice → query advice → report."""
    analyzer = PlanAnalyzer()
    index_advisor = IndexAdvisor()
    query_advisor = QueryAdvisor()

    console.print("[dim]Sorgu analiz ediliyor...[/dim]")

    try:
        plan_result = analyzer.analyze(db, sql, use_analyze=use_analyze)
    except QuerySyntaxError as e:
        if not use_ai:
            raise
        sql, plan_result = _handle_ai_syntax_fix(db, sql, e, use_analyze, analyzer)

    index_recs, unused = index_advisor.advise(plan_result, db_conn=db)
    query_recs = query_advisor.advise(sql)

    print_full_report(
        sql=sql,
        plan_result=plan_result,
        index_recs=index_recs,
        unused=unused,
        query_recs=query_recs,
        db_name=db.get_current_database(),
        server_version=db.server_version,
        show_sql=show_sql,
    )

    if use_ai:
        _print_ai_rewrite_suggestion(db, sql, plan_result, use_analyze)


def _handle_ai_syntax_fix(db, sql, syntax_err, use_analyze, analyzer):
    """
    A real PostgreSQL syntax error (SQLSTATE 42601) — the query doesn't even
    parse, so no rule-based rewrite can help. Ask Claude for a fix, then
    re-EXPLAIN the corrected query to verify it now actually parses/runs.
    Re-raises the ORIGINAL error if AI is unavailable or can't produce a
    verified fix, so exit codes and error reporting stay unchanged for
    non-AI use.
    """
    advisor = LLMAdvisor()
    reason = advisor.unavailable_reason()
    if reason:
        console.print(f"[yellow]AI destekli düzeltme kullanılamıyor:[/yellow] {reason}")
        raise syntax_err

    console.print("[dim]Sözdizimi hatası tespit edildi, AI destekli düzeltme deneniyor...[/dim]")
    try:
        suggestion = advisor.fix_syntax_error(sql, str(syntax_err), db.server_version)
    except RuntimeError as ai_err:
        console.print(f"[yellow]AI hatası:[/yellow] {ai_err}")
        raise syntax_err
    if suggestion is None:
        console.print("[yellow]AI güvenilir bir düzeltme öneremedi.[/yellow]")
        raise syntax_err

    try:
        plan_result = analyzer.analyze(db, suggestion.corrected_sql, use_analyze=use_analyze)
    except Exception as fix_err:
        console.print(f"[yellow]AI'nin önerdiği düzeltme de hata verdi:[/yellow] {fix_err}")
        console.print(Panel(
            suggestion.corrected_sql,
            title="[dim]AI önerisi (doğrulanamadı)[/dim]",
            box=box.ROUNDED,
        ))
        raise syntax_err

    console.print(f"[green]✓ AI düzeltmesi EXPLAIN ile doğrulandı[/green] — {suggestion.explanation}")
    console.print(Panel(
        suggestion.corrected_sql,
        title="[bold cyan]Düzeltilmiş Sorgu[/bold cyan]",
        box=box.ROUNDED,
    ))
    console.print(
        "[dim]Not: Bu düzeltme veritabanında geçerli bir plan ürettiği için doğrulandı; "
        "orijinal sorguyla aynı sonucu döndürdüğü garanti değildir.[/dim]"
    )
    return suggestion.corrected_sql, plan_result


def _print_ai_rewrite_suggestion(db, sql, plan_result, use_analyze) -> None:
    """
    Beyond syntax repair, optionally ask Claude for an alternative query
    structure (e.g. correlated subquery -> CTE) for problems the rule-based
    query_advisor doesn't already have a mechanical rewrite for. Verified via
    the same real-EXPLAIN pipeline as rule-based rewrites before it's shown.
    """
    advisor = LLMAdvisor()
    if advisor.unavailable_reason():
        return

    findings_summary = "\n".join(
        f"- [{f.level}] {f.title}: {f.description.splitlines()[0]}"
        for f in plan_result.findings if f.level in ("CRITICAL", "WARNING")
    )
    if not findings_summary:
        return

    plan_summary = f"Kök düğüm: {plan_result.root_node.node_type}, planlayıcı maliyeti {plan_result.root_node.total_cost:.1f}"
    if plan_result.has_actual:
        plan_summary += f", gerçek süre {plan_result.execution_time:.1f}ms"

    console.print()
    console.print("[dim]AI'den alternatif sorgu yapısı isteniyor...[/dim]")
    try:
        suggestion = advisor.suggest_rewrite(sql, plan_summary, findings_summary, db.server_version)
    except RuntimeError as e:
        console.print(f"[yellow]AI hatası:[/yellow] {e}")
        return
    if suggestion is None:
        return

    verification = verify_rewrite(db, plan_result, suggestion.corrected_sql, use_analyze)
    if verification is None:
        return  # not actually cheaper / doesn't verify — don't show unverified guesses

    console.print(Panel(
        suggestion.corrected_sql,
        title="[bold magenta]🤖 AI Sorgu Önerisi (deneysel)[/bold magenta]",
        box=box.ROUNDED,
    ))
    console.print(f"[dim]{suggestion.explanation}[/dim]")
    if verification.verified_with == "analyze":
        console.print(
            f"[green]📈 EXPLAIN ANALYZE ile doğrulandı:[/green] gerçek süre "
            f"{verification.before}ms → {verification.after}ms (%{verification.improvement_pct} azalma)"
        )
    else:
        console.print(
            f"[green]📈 EXPLAIN ile doğrulandı:[/green] planlayıcı maliyeti "
            f"{verification.before} → {verification.after} (%{verification.improvement_pct} azalma)"
        )
    console.print(
        "[yellow]⚠ Bu öneri veritabanında geçerli ve daha ucuz çalıştığı için doğrulandı; "
        "orijinal sorguyla AYNI sonuç kümesini döndürdüğü garanti değildir — kullanmadan önce "
        "sonuçları karşılaştırın.[/yellow]"
    )


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

@click.group()
@click.version_option(__version__, prog_name="analyzer4pg")
def main():
    """
    analyzer4pg — PostgreSQL Sorgu Analiz Aracı

    Oracle Tuning Advisor tarzında execution plan analizi,
    index önerileri ve sorgu yeniden yazım tavsiyeleri sunar.

    Örnekler:

      analyzer4pg analyze -H localhost -d mydb -U postgres \\
          -q "SELECT * FROM orders WHERE customer_id = 5"

      analyzer4pg repl -H localhost -d mydb -U postgres
    """
    pass


@main.command("analyze")
@add_connection_options
@click.option("-q", "--query",  default=None, help="Analiz edilecek SQL sorgusu")
@click.option("-f", "--file",   default=None, type=click.Path(exists=True), help="SQL sorgusu içeren dosya")
def analyze_cmd(host, port, dbname, user, password, sslmode, no_analyze, use_ai, query, file):
    """Tek bir sorguyu analiz et ve rapor üret."""
    # Resolve SQL
    if file:
        with open(file, "r", encoding="utf-8") as fh:
            sql = fh.read().strip()
    elif query:
        sql = query.strip()
    elif not sys.stdin.isatty():
        sql = sys.stdin.read().strip()
    else:
        console.print("[red]Hata:[/red] -q ile sorgu veya -f ile dosya belirtmelisiniz.")
        console.print("  Örnek: analyzer4pg analyze -q \"SELECT * FROM mytable\"")
        sys.exit(1)

    if not sql:
        console.print("[red]Hata:[/red] SQL sorgusu boş.")
        sys.exit(1)

    try:
        db = _connect(host, port, dbname, user, password, sslmode)
    except ConnectionError as e:
        console.print(f"[red]Bağlantı hatası:[/red] {e}")
        sys.exit(1)

    try:
        _run_analysis(db, sql, use_analyze=not no_analyze, use_ai=use_ai)
    except RuntimeError as e:
        console.print(f"[red]Analiz hatası:[/red] {e}")
        sys.exit(1)
    finally:
        db.close()


@main.command("repl")
@add_connection_options
def repl_cmd(host, port, dbname, user, password, sslmode, no_analyze, use_ai):
    """İnteraktif SQL analiz kabuğu (REPL) başlat."""
    try:
        db = _connect(host, port, dbname, user, password, sslmode)
    except ConnectionError as e:
        console.print(f"[red]Bağlantı hatası:[/red] {e}")
        sys.exit(1)

    db_name = db.get_current_database()
    print_header(db_name, db.server_version)

    _print_repl_help()

    buffer = []
    prompt_main = f"[bold cyan]analyzer4pg[/bold cyan] [dim]({db_name})[/dim][bold]>[/bold] "
    prompt_cont = "  [dim]...[/dim] "

    while True:
        try:
            is_continuation = len(buffer) > 0
            prompt = prompt_cont if is_continuation else prompt_main
            line = Prompt.ask(prompt, default="", show_default=False)
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Çıkılıyor...[/dim]")
            break

        stripped = line.strip()

        # --- meta commands ---
        if not is_continuation:
            if stripped.lower() in ("\\q", "\\quit", "exit", "quit"):
                console.print("[dim]Güle güle![/dim]")
                break

            if stripped.lower() in ("\\h", "\\help", "help"):
                _print_repl_help()
                continue

            if stripped.lower() in ("\\c", "\\clear"):
                console.clear()
                continue

            if stripped.lower().startswith("\\c "):
                # Reconnect: \c dbname or \c host/dbname/user
                new_db = stripped[3:].strip()
                try:
                    db.close()
                    cfg = build_connection_config(host, port, new_db, user, password, sslmode)
                    db = DatabaseConnection(cfg)
                    db.connect()
                    db_name = db.get_current_database()
                    prompt_main = f"[bold cyan]analyzer4pg[/bold cyan] [dim]({db_name})[/dim][bold]>[/bold] "
                    console.print(f"[green]✓ Bağlandı:[/green] {db_name}")
                except ConnectionError as e:
                    console.print(f"[red]Bağlantı hatası:[/red] {e}")
                continue

            if stripped.lower() == "\\analyze on":
                no_analyze = False
                console.print("[green]ANALYZE açık.[/green]")
                continue

            if stripped.lower() == "\\analyze off":
                no_analyze = True
                console.print("[yellow]ANALYZE kapalı (sadece EXPLAIN).[/yellow]")
                continue

            if stripped.lower() == "\\ai on":
                use_ai = True
                console.print("[green]AI destekli düzeltme açık.[/green]")
                continue

            if stripped.lower() == "\\ai off":
                use_ai = False
                console.print("[yellow]AI destekli düzeltme kapalı.[/yellow]")
                continue

        # --- SQL accumulation ---
        if stripped:
            buffer.append(line)

        # Execute when semicolon is found or buffer has content and user sent empty line
        full_sql = " ".join(buffer).strip()
        if full_sql.rstrip(";") and (
            stripped.endswith(";") or (not stripped and buffer)
        ):
            sql = full_sql.rstrip(";").strip()
            buffer = []
            if not sql:
                continue
            try:
                _run_analysis(db, sql, use_analyze=not no_analyze, show_sql=False, use_ai=use_ai)
            except RuntimeError as e:
                console.print(f"[red]Analiz hatası:[/red] {e}")
            except Exception as e:
                console.print(f"[red]Beklenmeyen hata:[/red] {e}")
        elif not stripped and not buffer:
            # Empty line with empty buffer — ignore
            pass

    db.close()


def _print_repl_help() -> None:
    console.print(Panel(
        "[bold]Komutlar:[/bold]\n"
        "  [cyan]\\q[/cyan] veya [cyan]quit[/cyan]    — Çıkış\n"
        "  [cyan]\\h[/cyan]             — Bu yardım mesajını göster\n"
        "  [cyan]\\c <dbname>[/cyan]    — Başka bir veritabanına bağlan\n"
        "  [cyan]\\clear[/cyan]         — Ekranı temizle\n"
        "  [cyan]\\analyze on/off[/cyan]— ANALYZE modunu aç/kapat\n"
        "  [cyan]\\ai on/off[/cyan]     — AI destekli düzeltme/öneri modunu aç/kapat\n\n"
        "[bold]SQL Kullanımı:[/bold]\n"
        "  Sorguyu yazın ve [bold]noktalı virgül (;)[/bold] ile bitirin:\n"
        "  [dim]SELECT * FROM orders WHERE customer_id = 5;[/dim]\n\n"
        "  Çok satırlı sorgular için Enter'a basın, son satıra ; ekleyin.\n"
        "  Boş satır göndermek de sorguyu çalıştırır.",
        title="[bold cyan]analyzer4pg REPL Yardım[/bold cyan]",
        box=box.ROUNDED,
        padding=(0, 2),
    ))
    console.print()


# ---------------------------------------------------------------------------
# Web command
# ---------------------------------------------------------------------------

@main.command("web")
@click.option("--host", "web_host", default="127.0.0.1", show_default=True, help="Dinlenecek adres (0.0.0.0 = tüm arayüzler)")
@click.option("--port", "web_port", default=5000, show_default=True, type=int, help="Port")
@click.option("--no-browser", is_flag=True, default=False, help="Tarayıcıyı otomatik açma")
def web_cmd(web_host, web_port, no_browser):
    """Web arayüzünü başlat ve tarayıcıda aç."""
    try:
        from .web.app import app as flask_app
    except ImportError:
        console.print("[red]Hata:[/red] Flask bulunamadı. Kurun: pip install flask")
        import sys; sys.exit(1)

    import threading, webbrowser, time

    url = f"http://{web_host if web_host != '0.0.0.0' else '127.0.0.1'}:{web_port}"

    console.print()
    console.print("[bold cyan]🐘 analyzer4pg[/bold cyan] web arayüzü başlatılıyor...")
    console.print(f"   [green]→[/green] {url}")
    console.print("   [dim]Durdurmak için: Ctrl+C[/dim]")
    console.print()

    if not no_browser:
        def _open():
            time.sleep(1.2)
            webbrowser.open(url)
        threading.Thread(target=_open, daemon=True).start()

    flask_app.run(host=web_host, port=web_port, debug=False, use_reloader=False)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
