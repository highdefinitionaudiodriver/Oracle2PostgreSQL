"""
Oracle2PostgreSQL - Report Generator
Generates HTML and CSV migration impact analysis reports.
"""

import csv
import os
from datetime import datetime
from typing import List, Dict

from .postgres_transformer import TransformResult, ChangeRecord


class ReportGenerator:
    """Generates HTML + CSV migration reports."""

    def __init__(self, output_dir: str, lang: str = "en"):
        self.output_dir = output_dir
        self.lang = lang

    def generate(self, results: List[TransformResult]) -> Dict[str, str]:
        """Generate HTML and CSV reports. Returns dict of report paths."""
        os.makedirs(self.output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_path = os.path.join(self.output_dir, f"migration_report_{timestamp}.html")
        csv_path = os.path.join(self.output_dir, f"migration_report_{timestamp}.csv")

        self._generate_html(results, html_path)
        self._generate_csv(results, csv_path)

        return {"html": html_path, "csv": csv_path}

    def _generate_html(self, results: List[TransformResult], path: str):
        """Generate HTML report."""
        total_files = len(results)
        total_auto = sum(r.auto_converted for r in results)
        total_review = sum(r.needs_review for r in results)
        total_manual = sum(r.manual_only for r in results)
        total_changes = total_auto + total_review + total_manual
        total_errors = sum(len(r.errors) for r in results)

        # Aggregate by category
        category_counts: Dict[str, Dict[str, int]] = {}
        for r in results:
            for c in r.changes:
                if c.category not in category_counts:
                    category_counts[c.category] = {"AUTO": 0, "REVIEW": 0, "MANUAL": 0}
                category_counts[c.category][c.severity] = \
                    category_counts[c.category].get(c.severity, 0) + 1

        # All review/manual items
        review_items = []
        for r in results:
            for c in r.changes:
                if c.severity in ("REVIEW", "MANUAL"):
                    review_items.append((r.filename, c))

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_ja = self.lang == "ja"

        title = "Oracle→PostgreSQL 移行レポート" if is_ja else "Oracle → PostgreSQL Migration Report"
        summary_title = "概要サマリー" if is_ja else "Executive Summary"
        cat_title = "カテゴリ別変更" if is_ja else "Changes by Category"
        review_title = "要確認・手動対応一覧" if is_ja else "Review / Manual Items"
        detail_title = "ファイル別詳細" if is_ja else "File Details"

        html = f"""<!DOCTYPE html>
<html lang="{self.lang}">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
    body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background: #f5f5f5; color: #333; }}
    h1 {{ color: #1a5276; border-bottom: 3px solid #2e86c1; padding-bottom: 10px; }}
    h2 {{ color: #1a5276; margin-top: 30px; }}
    .summary {{ display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; }}
    .card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); min-width: 150px; text-align: center; }}
    .card .number {{ font-size: 2em; font-weight: bold; }}
    .card .label {{ color: #666; margin-top: 5px; }}
    .card.auto .number {{ color: #27ae60; }}
    .card.review .number {{ color: #f39c12; }}
    .card.manual .number {{ color: #e74c3c; }}
    .card.total .number {{ color: #2e86c1; }}
    table {{ border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin: 15px 0; }}
    th {{ background: #2e86c1; color: white; padding: 12px; text-align: left; }}
    td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
    tr:hover {{ background: #f0f7fd; }}
    .severity-AUTO {{ color: #27ae60; font-weight: bold; }}
    .severity-REVIEW {{ color: #f39c12; font-weight: bold; }}
    .severity-MANUAL {{ color: #e74c3c; font-weight: bold; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; font-weight: bold; }}
    .badge-AUTO {{ background: #d5f5e3; color: #1e8449; }}
    .badge-REVIEW {{ background: #fdebd0; color: #b9770e; }}
    .badge-MANUAL {{ background: #fadbd8; color: #c0392b; }}
    .footer {{ margin-top: 40px; color: #999; font-size: 0.9em; text-align: center; }}
    .error {{ color: #e74c3c; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p>{"生成日時" if is_ja else "Generated"}: {now}</p>

<h2>{summary_title}</h2>
<div class="summary">
    <div class="card total">
        <div class="number">{total_files}</div>
        <div class="label">{"ファイル数" if is_ja else "Files"}</div>
    </div>
    <div class="card total">
        <div class="number">{total_changes}</div>
        <div class="label">{"総変更数" if is_ja else "Total Changes"}</div>
    </div>
    <div class="card auto">
        <div class="number">{total_auto}</div>
        <div class="label">{"自動変換" if is_ja else "Auto-converted"}</div>
    </div>
    <div class="card review">
        <div class="number">{total_review}</div>
        <div class="label">{"要確認" if is_ja else "Needs Review"}</div>
    </div>
    <div class="card manual">
        <div class="number">{total_manual}</div>
        <div class="label">{"手動対応" if is_ja else "Manual"}</div>
    </div>
    <div class="card {'error' if total_errors else ''}">
        <div class="number">{total_errors}</div>
        <div class="label">{"エラー" if is_ja else "Errors"}</div>
    </div>
</div>

<h2>{cat_title}</h2>
<table>
<tr>
    <th>{"カテゴリ" if is_ja else "Category"}</th>
    <th>AUTO</th>
    <th>REVIEW</th>
    <th>MANUAL</th>
    <th>{"合計" if is_ja else "Total"}</th>
</tr>"""

        for cat, counts in sorted(category_counts.items()):
            cat_total = sum(counts.values())
            html += f"""
<tr>
    <td><strong>{cat}</strong></td>
    <td class="severity-AUTO">{counts.get('AUTO', 0)}</td>
    <td class="severity-REVIEW">{counts.get('REVIEW', 0)}</td>
    <td class="severity-MANUAL">{counts.get('MANUAL', 0)}</td>
    <td><strong>{cat_total}</strong></td>
</tr>"""

        html += """
</table>"""

        # Review/Manual items
        if review_items:
            html += f"""
<h2>{review_title}</h2>
<table>
<tr>
    <th>{"ファイル" if is_ja else "File"}</th>
    <th>{"深刻度" if is_ja else "Severity"}</th>
    <th>{"カテゴリ" if is_ja else "Category"}</th>
    <th>{"ルールID" if is_ja else "Rule ID"}</th>
    <th>{"説明" if is_ja else "Description"}</th>
    <th>{"行" if is_ja else "Line"}</th>
</tr>"""

            for fname, change in review_items:
                desc = change.description_ja if is_ja and change.description_ja else change.description
                html += f"""
<tr>
    <td>{os.path.basename(fname)}</td>
    <td><span class="badge badge-{change.severity}">{change.severity}</span></td>
    <td>{change.category}</td>
    <td>{change.rule_id}</td>
    <td>{desc}</td>
    <td>{change.line_number}</td>
</tr>"""

            html += """
</table>"""

        # File details
        html += f"""
<h2>{detail_title}</h2>"""

        for r in results:
            fname = os.path.basename(r.filename) if r.filename else "unknown"
            html += f"""
<h3>{fname}</h3>
<p>{"自動" if is_ja else "Auto"}: {r.auto_converted} |
{"要確認" if is_ja else "Review"}: {r.needs_review} |
{"手動" if is_ja else "Manual"}: {r.manual_only}</p>"""

            if r.changes:
                html += """<table>
<tr><th>Rule ID</th><th>Severity</th><th>Category</th><th>Description</th></tr>"""
                for c in r.changes:
                    desc = c.description_ja if is_ja and c.description_ja else c.description
                    html += f"""
<tr>
    <td>{c.rule_id}</td>
    <td><span class="badge badge-{c.severity}">{c.severity}</span></td>
    <td>{c.category}</td>
    <td>{desc}</td>
</tr>"""
                html += "</table>"

            if r.errors:
                html += f'<p class="error">{"エラー" if is_ja else "Errors"}:</p><ul>'
                for e in r.errors:
                    html += f"<li class='error'>{e}</li>"
                html += "</ul>"

        html += f"""
<div class="footer">
    <p>Oracle → PostgreSQL Migration Tool | {now}</p>
</div>
</body>
</html>"""

        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(html)

    def _generate_csv(self, results: List[TransformResult], path: str):
        """Generate CSV report."""
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "File", "Rule ID", "Category", "Severity",
                "Description", "Description (JA)", "Line"
            ])
            for r in results:
                fname = os.path.basename(r.filename) if r.filename else ""
                for c in r.changes:
                    writer.writerow([
                        fname, c.rule_id, c.category, c.severity,
                        c.description, c.description_ja, c.line_number
                    ])
