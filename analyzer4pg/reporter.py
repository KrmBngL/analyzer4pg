"""
reporter.py - Rich-based terminal report output for analyzer4pg
"""

from __future__ import annotations

from typing import List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from rich.syntax import Syntax
from rich.text import Text
from rich.columns import Columns
from rich import box
from rich.rule import Rule

from .plan_analyzer import PlanResult, PlanNode, Finding
from .index_advisor import IndexRecommendation, UnusedIndexWarning
from .query_advisor import QueryRecommendation

console = Console()


# ---------------------------------------------------------------------------
# Color / style helpers
# ---------------------------------------------------------------------------

LEVEL_STYLE = {
    "CRITICAL": "bold red",
    "WARNING":  "bold yellow",
    "INFO":     "cyan",
    "OK":       "green",
}

PRIORITY_STYLE = {
    "HIGH":   "bold red",
    "MEDIUM": "bold yellow",
    "LOW":    "dim cyan",
}

GRADE_STYLE = {
    "A": "bold green",
    "B": "green",
    "C": "yellow",
    "D": "bold red",
    "F": "bold red on white",
}


def _score_bar(score: int, width: int = 30) -> Text:
    filled = int(score / 100 * width)
    bar = Text()
    bar.append("█" * filled, style="green" if score >= 75 else "yellow" if score >= 50 else "red")
    bar.append("░" * (width - filled), style="dim")
    return bar


def _node_style(node: PlanNode, root_cost: float) -> str:
    if node.node_type == "Seq Scan":
        rows = node.actual_rows if node.has_actual else node.plan_rows
        if rows >= 1000:
            return "bold red"
        return "yellow"
    if root_cost > 0 and node.total_cost / root_cost > 0.5:
        return "bold yellow"
    if "Index" in node.node_type:
        return "green"
    return "white"


def _node_label(node: PlanNode) -> Text:
    t = Text()

    style = _node_style(node, getattr(node, "_root_cost", node.total_cost))
    t.append(node.node_type, style=style)

    if node.relation_name:
        t.append(f" on ", style="dim")
        t.append(node.relation_name, style="bold cyan")
        if node.alias and node.alias != node.relation_name:
            t.append(f" ({node.alias})", style="dim")

    if node.index_name:
        t.append(" [", style="dim")
        t.append(node.index_name, style="italic green")
        t.append("]", style="dim")

    # Cost
    t.append(f"  cost={node.startup_cost:.2f}..{node.total_cost:.2f}", style="dim")

    # Actual time + rows
    if node.has_actual:
        t.append(f"  actual={node.actual_total_time:.3f}ms", style="dim")
        t.append(f"  rows={node.actual_rows:,}", style="dim")
        # Estimation warning
        ratio = node.row_estimation_ratio
        if ratio > 10 or (ratio < 0.1 and node.actual_rows > 0):
            t.append(f" ⚠ est={node.plan_rows:,}", style="bold yellow")
    else:
        t.append(f"  rows≈{node.plan_rows:,}", style="dim")

    # Filter
    if node.filter:
        short_filter = node.filter[:60] + "..." if len(node.filter) > 60 else node.filter
        t.append(f"\n    Filter: {short_filter}", style="italic dim")

    if node.index_cond:
        short = node.index_cond[:60] + "..." if len(node.index_cond) > 60 else node.index_cond
        t.append(f"\n    Index Cond: {short}", style="italic dim")

    if node.hash_cond:
        short = node.hash_cond[:60] + "..." if len(node.hash_cond) > 60 else node.hash_cond
        t.append(f"\n    Hash Cond: {short}", style="italic dim")

    # Sort spill indicator
    if node.sort_method and "disk" in node.sort_method.lower():
        t.append(f"  ⚠ DISK SORT ({node.sort_space_used:,} kB)", style="bold red")

    # Hash spill indicator
    if node.hash_batches > 1:
        t.append(f"  ⚠ HASH SPILL ({node.hash_batches} batches)", style="bold red")

    return t


def _build_tree(node: PlanNode, rich_tree: Tree, root_cost: float) -> None:
    node._root_cost = root_cost
    label = _node_label(node)
    branch = rich_tree.add(label)
    for child in node.children:
        _build_tree(child, branch, root_cost)


# ---------------------------------------------------------------------------
# Section printers
# ---------------------------------------------------------------------------

def print_header(db_name: str, server_version: str) -> None:
    console.print()
    console.print(Panel(
        f"[bold cyan]analyzer4pg[/bold cyan] — PostgreSQL Sorgu Analiz Aracı\n"
        f"[dim]Veritabanı:[/dim] [cyan]{db_name}[/cyan]   "
        f"[dim]Sunucu:[/dim] [cyan]{server_version[:50]}[/cyan]",
        box=box.DOUBLE_EDGE,
        style="bold",
        padding=(0, 2),
    ))
    console.print()


def print_formatted_sql(sql: str) -> None:
    from .query_advisor import format_sql
    console.print(Rule("[bold]Sorgu[/bold]", style="blue"))
    formatted = format_sql(sql)
    console.print(Syntax(formatted, "sql", theme="monokai", line_numbers=True, word_wrap=True))
    console.print()


def print_plan_tree(plan_result: PlanResult) -> None:
    console.print(Rule("[bold]Execution Plan[/bold]", style="blue"))

    root = plan_result.root_node
    root_cost = root.total_cost if root.total_cost > 0 else 1.0

    # Timing summary
    info_text = Text()
    if plan_result.has_actual:
        info_text.append(f"  Planning: ", style="dim")
        info_text.append(f"{plan_result.planning_time:.3f}ms", style="cyan")
        info_text.append(f"   Execution: ", style="dim")
        info_text.append(f"{plan_result.execution_time:.3f}ms", style="bold cyan")
        info_text.append(f"   Total: ", style="dim")
        total = plan_result.planning_time + plan_result.execution_time
        info_text.append(f"{total:.3f}ms", style="bold cyan")
    else:
        info_text.append("  (EXPLAIN only — no actual execution times)", style="dim italic")
    console.print(info_text)
    console.print()

    tree = Tree(_node_label(root))
    root._root_cost = root_cost
    for child in root.children:
        _build_tree(child, tree, root_cost)

    console.print(tree)
    console.print()


def print_findings(findings: List[Finding]) -> None:
    if not findings:
        console.print(Rule("[bold green]Plan Bulguları[/bold green]", style="green"))
        console.print("  [green]✓ Plan analizi sorun tespit etmedi.[/green]\n")
        return

    console.print(Rule(f"[bold]Plan Bulguları ({len(findings)})[/bold]", style="yellow"))

    for i, f in enumerate(findings, 1):
        level_style = LEVEL_STYLE.get(f.level, "white")
        badge = f"[{level_style}][{f.level}][/{level_style}]"

        node_ref = ""
        if f.node and f.node.relation_name:
            node_ref = f" [dim]→ {f.node.node_type} on {f.node.relation_name}[/dim]"

        console.print(f"  {i}. {badge} [bold]{f.title}[/bold]{node_ref}")
        for line in f.description.split("\n"):
            console.print(f"     {line}", style="dim")
        console.print(f"     [italic cyan]Öneri:[/italic cyan] ", end="")
        for line in f.recommendation.split("\n"):
            console.print(f"[italic]{line}[/italic]")
        console.print()


def print_index_recommendations(
    recs: List[IndexRecommendation],
    unused: List[UnusedIndexWarning],
) -> None:
    console.print(Rule("[bold]Index Önerileri[/bold]", style="blue"))

    if not recs and not unused:
        console.print("  [green]✓ Mevcut indexler yeterli görünüyor.[/green]\n")
        return

    if recs:
        console.print(f"  [bold]{len(recs)} yeni index önerisi:[/bold]\n")
        for i, rec in enumerate(recs, 1):
            pri_style = PRIORITY_STYLE.get(rec.priority, "white")
            console.print(
                f"  {i}. [{pri_style}][{rec.priority}][/{pri_style}] "
                f"[bold]{rec.schema}.{rec.table}[/bold] "
                f"([cyan]{', '.join(rec.columns)}[/cyan])"
                + (" [dim italic](partial)[/dim italic]" if rec.is_partial else "")
            )
            console.print(f"     [dim]Neden:[/dim] {rec.reason.split(chr(10))[0]}")
            console.print(f"     [dim]Etki:[/dim]  {rec.impact.split(chr(10))[0]}")
            console.print()
            console.print(Syntax(rec.ddl, "sql", theme="monokai", padding=(0, 5)))
            console.print()

    if unused:
        console.print(f"  [bold yellow]{len(unused)} kullanılmayan index:[/bold yellow]\n")
        tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold")
        tbl.add_column("Index")
        tbl.add_column("Tablo")
        tbl.add_column("Boyut")
        tbl.add_column("Öneri")
        for u in unused:
            tbl.add_row(
                u.index_name,
                f"{u.schema}.{u.table}",
                u.index_size,
                f"[dim]DROP INDEX CONCURRENTLY {u.index_name};[/dim]",
            )
        console.print(tbl)
        console.print()


def print_query_recommendations(recs: List[QueryRecommendation]) -> None:
    console.print(Rule("[bold]Sorgu Önerileri[/bold]", style="blue"))

    if not recs:
        console.print("  [green]✓ Sorguda belirgin anti-pattern tespit edilmedi.[/green]\n")
        return

    for i, rec in enumerate(recs, 1):
        pri_style = PRIORITY_STYLE.get(rec.priority, "white")
        cat_style = "bold magenta" if rec.category == "ANTIPATTERN" else "bold blue"

        console.print(
            f"  {i}. [{pri_style}][{rec.priority}][/{pri_style}] "
            f"[{cat_style}][{rec.category}][/{cat_style}] "
            f"[bold]{rec.title}[/bold]"
        )
        for line in rec.description.split("\n"):
            console.print(f"     {line}", style="dim")

        if rec.example_before:
            console.print()
            console.print("     [dim]Mevcut:[/dim]")
            console.print(Syntax(rec.example_before, "sql", theme="monokai", padding=(0, 5)))
        if rec.example_after:
            console.print("     [dim]Önerilen:[/dim]")
            console.print(Syntax(rec.example_after, "sql", theme="monokai", padding=(0, 5)))
        console.print()


def print_score_summary(
    plan_result: PlanResult,
    index_recs: List[IndexRecommendation],
    query_recs: List[QueryRecommendation],
    unused: List[UnusedIndexWarning],
) -> None:
    console.print(Rule("[bold]Performans Özeti[/bold]", style="bold blue"))

    score = plan_result.score
    grade = plan_result.grade
    grade_style = GRADE_STYLE.get(grade, "white")

    # Additional deductions from query advisor
    query_deduction = sum(r.score_impact for r in query_recs)
    final_score = max(0, score - query_deduction)
    for threshold, g, _ in [
        (90, "A", "Mükemmel"), (75, "B", "İyi"),
        (60, "C", "Orta"), (40, "D", "Zayıf"), (0, "F", "Kritik"),
    ]:
        if final_score >= threshold:
            grade = g
            break
    grade_style = GRADE_STYLE.get(grade, "white")

    # Grade description
    grade_desc = {
        "A": "Mükemmel — Sorgu iyi optimize edilmiş",
        "B": "İyi — Küçük iyileştirmeler yapılabilir",
        "C": "Orta — İyileştirme önerilir",
        "D": "Zayıf — Ciddi performans sorunları var",
        "F": "Kritik — Acil aksiyon gerekli",
    }.get(grade, "")

    # Score display
    bar = _score_bar(final_score)
    score_text = Text()
    score_text.append(f"\n  Performans Skoru: ", style="bold")
    score_text.append(f"{final_score}/100 ", style=grade_style + " bold")
    score_text.append("  ")
    score_text.append(bar)
    score_text.append(f"  [{grade_style}]Not: {grade}[/{grade_style}] ", style="")
    score_text.append(f"— {grade_desc}", style="dim")
    console.print(score_text)
    console.print()

    # Breakdown table
    tbl = Table(box=box.SIMPLE, show_header=True, header_style="dim")
    tbl.add_column("Kategori", style="bold")
    tbl.add_column("Bulgu", justify="right")
    tbl.add_column("Skor Etkisi", justify="right")

    critical = [f for f in plan_result.findings if f.level == "CRITICAL"]
    warnings  = [f for f in plan_result.findings if f.level == "WARNING"]
    infos     = [f for f in plan_result.findings if f.level == "INFO"]

    if critical:
        tbl.add_row("Plan — Kritik Sorunlar", str(len(critical)),
                    f"[red]-{sum(f.score_impact for f in critical)}[/red]")
    if warnings:
        tbl.add_row("Plan — Uyarılar", str(len(warnings)),
                    f"[yellow]-{sum(f.score_impact for f in warnings)}[/yellow]")
    if infos:
        tbl.add_row("Plan — Bilgilendirme", str(len(infos)),
                    f"[cyan]-{sum(f.score_impact for f in infos)}[/cyan]")
    if query_recs:
        tbl.add_row("Sorgu Anti-Pattern", str(len(query_recs)),
                    f"[yellow]-{query_deduction}[/yellow]")
    if index_recs:
        tbl.add_row("Eksik Index", str(len(index_recs)), "[dim]—[/dim]")
    if unused:
        tbl.add_row("Kullanılmayan Index", str(len(unused)), "[dim]—[/dim]")

    console.print(tbl)

    # Timing summary
    if plan_result.has_actual:
        total_ms = plan_result.planning_time + plan_result.execution_time
        console.print(
            f"  [dim]Toplam sorgu süresi:[/dim] "
            f"[bold]{total_ms:.3f}ms[/bold]  "
            f"[dim](planlama: {plan_result.planning_time:.3f}ms + "
            f"çalışma: {plan_result.execution_time:.3f}ms)[/dim]"
        )

    console.print()

    # Action items
    action_items = []
    for f in critical + warnings[:3]:
        action_items.append(("  ► ", f.title, "red" if f.level == "CRITICAL" else "yellow"))
    for r in index_recs[:3]:
        action_items.append(("  ► ", f"Index ekle: {r.schema}.{r.table}({', '.join(r.columns)})", "cyan"))
    for r in query_recs[:2]:
        action_items.append(("  ► ", r.title, "magenta"))

    if action_items:
        console.print("  [bold]Öncelikli Aksiyonlar:[/bold]")
        for prefix, text, style in action_items:
            console.print(f"{prefix}[{style}]{text}[/{style}]")
        console.print()


def print_full_report(
    sql: str,
    plan_result: PlanResult,
    index_recs: List[IndexRecommendation],
    unused: List[UnusedIndexWarning],
    query_recs: List[QueryRecommendation],
    db_name: str = "",
    server_version: str = "",
    show_sql: bool = True,
) -> None:
    """Print the complete analysis report to the terminal."""
    if db_name:
        print_header(db_name, server_version)

    if show_sql:
        print_formatted_sql(sql)

    print_plan_tree(plan_result)
    print_findings(plan_result.findings)
    print_index_recommendations(index_recs, unused)
    print_query_recommendations(query_recs)
    print_score_summary(plan_result, index_recs, query_recs, unused)
