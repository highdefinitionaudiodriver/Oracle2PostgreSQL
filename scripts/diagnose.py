#!/usr/bin/env python3
"""
Oracle → PostgreSQL 移行 事前診断スクリプト

既存の Oracle2PostgreSQL 変換パイプラインに依存しない単独ツールとして、
入力ディレクトリの `.sql` を走査し以下を出力します：

  - 全ファイルの AUTO / REVIEW / MANUAL 分類
  - 未対応構文ランキング Top N
  - 概算工数（時間／人日）
  - HTML サマリ（A4 印刷で 1〜2 枚に収まる形）

「実プロジェクトの移行前に、上司や顧客に出せる 1 ページの数字」を作るための
営業・見積もり用スクリプトです。

使い方:
    python scripts/diagnose.py <input_dir>                  # report.csv
    python scripts/diagnose.py <input_dir> --html report.html
    python scripts/diagnose.py <input_dir> -o estimate.csv
"""

from __future__ import annotations

import argparse
import csv
import html
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# ──────────────────────────────────────────────────────────────────────────
# 判定ルール
#
# 各パターンは「カテゴリ」「正規表現」「重み（工数係数）」「説明」のタプル。
# AUTO は自動変換可能、REVIEW は構文変換は可能だが意味を要確認、
# MANUAL は設計変更が必要なもの。
# ──────────────────────────────────────────────────────────────────────────

AUTO_PATTERNS = [
    # データ型
    (r"\bVARCHAR2\s*\(", 1, "VARCHAR2 → VARCHAR"),
    (r"\bNVARCHAR2\s*\(", 1, "NVARCHAR2 → VARCHAR"),
    (r"\bNUMBER\s*\(", 1, "NUMBER(p,s) → NUMERIC(p,s)"),
    (r"\bCLOB\b", 1, "CLOB → TEXT"),
    (r"\bBLOB\b", 1, "BLOB → BYTEA"),
    (r"\bRAW\s*\(", 1, "RAW(n) → BYTEA"),
    (r"\bXMLTYPE\b", 1, "XMLTYPE → XML"),
    # 関数
    (r"\bSYSDATE\b", 1, "SYSDATE → CURRENT_TIMESTAMP"),
    (r"\bNVL\s*\(", 1, "NVL(...) → COALESCE(...)"),
    (r"\bSYS_GUID\s*\(", 1, "SYS_GUID() → gen_random_uuid()"),
    (r"\.\s*NEXTVAL\b", 1, ".NEXTVAL → nextval(...)"),
    (r"\.\s*CURRVAL\b", 1, ".CURRVAL → currval(...)"),
    # 構文
    (r"\bFROM\s+DUAL\b", 1, "FROM DUAL → 削除"),
    (r"/\*\+[^*]*\*/", 1, "Oracle ヒント → コメント化"),
    (r"\bTABLESPACE\s+\w+", 1, "TABLESPACE 句 → 削除"),
    (r"\bSTORAGE\s*\(", 1, "STORAGE 句 → 削除"),
    (r"\bMINUS\b", 1, "MINUS → EXCEPT"),
    (r"\bDBMS_OUTPUT\.PUT_LINE\b", 1, "DBMS_OUTPUT.PUT_LINE → RAISE NOTICE"),
]

REVIEW_PATTERNS = [
    (r"\(\s*\+\s*\)", 4, "(+) 旧式外部結合 → LEFT/RIGHT JOIN への書換要"),
    (r"\bDECODE\s*\(", 2, "DECODE(...) → CASE WHEN（パラメータ数で展開）"),
    (r"\bROWNUM\b", 3, "ROWNUM → ROW_NUMBER() OVER() 等への書換要"),
    (r"\bSTART\s+WITH\b", 4, "階層問合せ → WITH RECURSIVE 設計要"),
    (r"\bCONNECT\s+BY\b", 4, "階層問合せ → WITH RECURSIVE 設計要"),
    (r"\bTO_DATE\s*\(", 2, "TO_DATE → TO_TIMESTAMP / フォーマット互換確認"),
    (r"\bADD_MONTHS\s*\(", 2, "ADD_MONTHS → + INTERVAL 'n months'"),
    (r"\bMERGE\s+INTO\b", 3, "MERGE 構文 → PostgreSQL 15+ では対応、要構文確認"),
    (r":\s*NEW\.\w+", 2, ":NEW.col → NEW.col（PL/pgSQL）"),
    (r":\s*OLD\.\w+", 2, ":OLD.col → OLD.col（PL/pgSQL）"),
    (r"\bEXEC(UTE)?\s+IMMEDIATE\b", 3, "EXECUTE IMMEDIATE → EXECUTE（クォート差異あり）"),
    (r"\bRAISE_APPLICATION_ERROR\s*\(", 2, "RAISE_APPLICATION_ERROR → RAISE EXCEPTION"),
]

MANUAL_PATTERNS = [
    (r"\bCREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\b", 12, "PACKAGE → Schema + 個別関数への再設計"),
    (r"\bPRAGMA\s+AUTONOMOUS_TRANSACTION\b", 8, "自律トランザクション → dblink 等で代替"),
    (r"\bBULK\s+COLLECT\b", 6, "BULK COLLECT → array_agg / FETCH 構造への書換"),
    (r"\bFORALL\b", 6, "FORALL → 配列バインドへの書換"),
    (r"\bDBMS_LOB\b", 4, "DBMS_LOB → PostgreSQL の lo_* 関数や TEXT/BYTEA 操作"),
    (r"\bDBMS_SQL\b", 8, "DBMS_SQL → PL/pgSQL EXECUTE / 別アプローチ"),
    (r"\bDBMS_SCHEDULER\b", 6, "DBMS_SCHEDULER → pg_cron 等の別途設定"),
    (r"\bUTL_FILE\b", 8, "UTL_FILE → サーバサイド COPY / 外部処理"),
    (r"\bUTL_HTTP\b", 6, "UTL_HTTP → 拡張 (http) または外部処理"),
    (r"\bUTL_SMTP\b", 6, "UTL_SMTP → 外部処理（pg_mail 等）"),
    (r"\bCREATE\s+SYNONYM\b", 3, "SYNONYM → ビューまたは search_path 設計"),
    (r"\bGLOBAL\s+TEMPORARY\s+TABLE\b", 4, "GLOBAL TEMP → TEMPORARY ON COMMIT DROP 設計"),
    (r"\bORGANIZATION\s+INDEX\b", 5, "Index-Organized Table → クラスタード設計"),
    (r"\bPARTITION\s+BY\s+", 4, "Partitioning → PostgreSQL 宣言的パーティション設計"),
    (r"\bMATERIALIZED\s+VIEW\b", 3, "MATERIALIZED VIEW → 構文同等だがリフレッシュ戦略要設計"),
]


@dataclass
class FileDiagnostic:
    path: str
    relative_path: str
    loc: int
    auto_hits: int = 0
    review_hits: int = 0
    manual_hits: int = 0
    weighted_effort_min: float = 0.0
    category: str = "AUTO"   # AUTO / REVIEW / MANUAL
    top_findings: list[tuple[str, int]] = field(default_factory=list)


def strip_comments(sql: str) -> str:
    """SQL コメント (-- 行末 / /* ... */ ブロック) を除去。文字列リテラル内は触らない。"""
    out: list[str] = []
    i, n = 0, len(sql)
    in_str = False
    while i < n:
        ch = sql[i]
        if not in_str:
            if ch == "-" and i + 1 < n and sql[i + 1] == "-":
                # 行コメント
                nl = sql.find("\n", i)
                if nl < 0:
                    break
                out.append("\n")
                i = nl + 1
                continue
            if ch == "/" and i + 1 < n and sql[i + 1] == "*":
                # ブロックコメント
                end = sql.find("*/", i + 2)
                if end < 0:
                    break
                i = end + 2
                continue
            if ch == "'":
                in_str = True
                out.append(ch)
                i += 1
                continue
            out.append(ch)
            i += 1
        else:
            if ch == "'":
                # '' エスケープ
                if i + 1 < n and sql[i + 1] == "'":
                    out.append("''")
                    i += 2
                    continue
                in_str = False
            out.append(ch)
            i += 1
    return "".join(out)


def read_text(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis", "euc_jp", "latin-1"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="latin-1", errors="replace")


def diagnose_file(path: Path, root: Path) -> FileDiagnostic:
    raw = read_text(path)
    code = strip_comments(raw)
    loc = code.count("\n") + 1
    findings: Counter[str] = Counter()
    effort = 0.0

    auto_hits = review_hits = manual_hits = 0
    for regex, weight_min, desc in AUTO_PATTERNS:
        n = len(re.findall(regex, code, re.IGNORECASE))
        if n:
            auto_hits += n
            effort += n * weight_min
            findings[desc] += n
    for regex, weight_min, desc in REVIEW_PATTERNS:
        n = len(re.findall(regex, code, re.IGNORECASE))
        if n:
            review_hits += n
            effort += n * weight_min
            findings[desc] += n
    for regex, weight_min, desc in MANUAL_PATTERNS:
        n = len(re.findall(regex, code, re.IGNORECASE))
        if n:
            manual_hits += n
            effort += n * weight_min
            findings[desc] += n

    # カテゴリ判定: MANUAL が 1 つでもあれば MANUAL、なければ REVIEW があるか、AUTO のみか
    if manual_hits > 0:
        category = "MANUAL"
    elif review_hits > 0:
        category = "REVIEW"
    else:
        category = "AUTO"

    try:
        rel = str(path.relative_to(root))
    except ValueError:
        rel = str(path)

    return FileDiagnostic(
        path=str(path),
        relative_path=rel,
        loc=loc,
        auto_hits=auto_hits,
        review_hits=review_hits,
        manual_hits=manual_hits,
        weighted_effort_min=round(effort, 1),
        category=category,
        top_findings=findings.most_common(5),
    )


def collect_files(root: Path) -> list[Path]:
    files = [p for p in root.rglob("*.sql") if p.is_file()]
    files += [p for p in root.rglob("*.SQL") if p.is_file()]
    # 重複（大小区別なし OS）を除去
    seen = set()
    out = []
    for p in sorted(files):
        rp = p.resolve()
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out


def write_csv(diags: list[FileDiagnostic], out_path: Path) -> None:
    headers = [
        "relative_path", "loc", "category",
        "auto_hits", "review_hits", "manual_hits",
        "estimated_effort_minutes", "top_findings",
    ]
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, lineterminator="\r\n")
        w.writerow(headers)
        for d in diags:
            top = "; ".join(f"{name}×{n}" for name, n in d.top_findings)
            w.writerow([
                d.relative_path, d.loc, d.category,
                d.auto_hits, d.review_hits, d.manual_hits,
                d.weighted_effort_min, top,
            ])


def aggregate(diags: list[FileDiagnostic]) -> dict:
    by_cat = Counter(d.category for d in diags)
    total_effort_min = sum(d.weighted_effort_min for d in diags)
    # 全findings の Top 10
    all_findings: Counter[str] = Counter()
    for d in diags:
        for name, n in d.top_findings:
            all_findings[name] += n
    return {
        "file_count": len(diags),
        "by_category": dict(by_cat),
        "total_loc": sum(d.loc for d in diags),
        "total_effort_min": round(total_effort_min, 1),
        "total_effort_hr": round(total_effort_min / 60.0, 1),
        "total_effort_pd": round(total_effort_min / 60.0 / 8.0, 1),
        "top_unsupported": all_findings.most_common(10),
    }


def write_html(diags: list[FileDiagnostic], out_path: Path) -> None:
    agg = aggregate(diags)
    by = agg["by_category"]
    auto_c = by.get("AUTO", 0)
    review_c = by.get("REVIEW", 0)
    manual_c = by.get("MANUAL", 0)
    n = max(1, agg["file_count"])

    def pct(c: int) -> str:
        return f"{c / n * 100:.1f}"

    top_rows = "".join(
        f"<tr><td>{i + 1}</td><td>{html.escape(name)}</td><td>{count}</td></tr>"
        for i, (name, count) in enumerate(agg["top_unsupported"])
    )

    detail_rows = "".join(
        f"<tr><td>{html.escape(d.relative_path)}</td>"
        f"<td>{d.loc}</td>"
        f"<td><span class='badge cat-{d.category.lower()}'>{d.category}</span></td>"
        f"<td>{d.weighted_effort_min:.0f} 分</td>"
        f"<td class='findings-cell'>{html.escape('; '.join(f'{nm}×{cn}' for nm, cn in d.top_findings))}</td></tr>"
        for d in sorted(diags, key=lambda x: x.weighted_effort_min, reverse=True)[:20]
    )

    body = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <title>Oracle → PostgreSQL 移行 事前診断レポート</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=Noto+Sans+JP:wght@300;400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --primary-color: #1e40af;
      --primary-gradient: linear-gradient(135deg, #1e40af, #1e3a8a);
      --bg-color: #f8fafc;
      --card-bg: #ffffff;
      --text-color: #1e293b;
      --text-muted: #64748b;
      --border-color: #e2e8f0;
      
      --color-auto: #15803d;
      --bg-auto: #dcfce7;
      --color-review: #c2410c;
      --bg-review: #ffedd5;
      --color-manual: #b91c1c;
      --bg-manual: #fee2e2;
    }}
    
    body {{
      font-family: 'Inter', 'Noto Sans JP', sans-serif;
      background-color: var(--bg-color);
      color: var(--text-color);
      line-height: 1.6;
      margin: 0;
      padding: 40px 20px;
    }}
    
    .container {{
      max-width: 1000px;
      margin: 0 auto;
    }}
    
    header {{
      background: var(--primary-gradient);
      color: #ffffff;
      padding: 35px 40px;
      border-radius: 16px;
      margin-bottom: 30px;
      box-shadow: 0 10px 25px -5px rgba(30, 64, 175, 0.15);
      position: relative;
      overflow: hidden;
    }}
    
    header::after {{
      content: "";
      position: absolute;
      top: -50%;
      right: -20%;
      width: 400px;
      height: 400px;
      background: rgba(255, 255, 255, 0.04);
      border-radius: 50%;
    }}
    
    .badge-report {{
      display: inline-block;
      background: rgba(255, 255, 255, 0.2);
      padding: 4px 12px;
      border-radius: 20px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 1px;
      margin-bottom: 12px;
    }}
    
    header h1 {{
      margin: 0;
      font-size: 26px;
      font-weight: 700;
      letter-spacing: -0.5px;
    }}
    
    header .subtitle {{
      margin-top: 8px;
      font-size: 14px;
      opacity: 0.9;
      font-weight: 300;
    }}
    
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 20px;
      margin-bottom: 30px;
    }}
    
    .kpi-card {{
      background: var(--card-bg);
      padding: 24px;
      border-radius: 16px;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
      border: 1px solid var(--border-color);
      transition: transform 0.2s, box-shadow 0.2s;
    }}
    
    .kpi-card:hover {{
      transform: translateY(-2px);
      box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
    }}
    
    .kpi-val {{
      font-size: 32px;
      font-weight: 700;
      color: var(--primary-color);
      line-height: 1.2;
    }}
    
    .kpi-label {{
      font-size: 12px;
      color: var(--text-muted);
      margin-top: 6px;
      font-weight: 600;
    }}
    
    section {{
      background: var(--card-bg);
      padding: 35px 40px;
      border-radius: 16px;
      margin-bottom: 30px;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
      border: 1px solid var(--border-color);
    }}
    
    section h2 {{
      font-size: 18px;
      font-weight: 700;
      margin-top: 0;
      margin-bottom: 20px;
      padding-bottom: 12px;
      border-bottom: 2px solid #f1f5f9;
      color: var(--text-color);
    }}
    
    .summary-bar {{
      height: 28px;
      display: flex;
      border-radius: 8px;
      overflow: hidden;
      box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.06);
      margin-bottom: 20px;
    }}
    
    .summary-bar > div {{
      line-height: 28px;
      color: #ffffff;
      text-align: center;
      font-size: 11px;
      font-weight: 700;
      transition: width 0.3s ease;
    }}
    
    .seg-auto {{ background-color: #22c55e; }}
    .seg-review {{ background-color: #f97316; }}
    .seg-manual {{ background-color: #ef4444; }}
    
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13.5px;
      text-align: left;
    }}
    
    th {{
      font-weight: 600;
      color: var(--text-color);
      border-bottom: 2px solid var(--border-color);
      padding: 12px 16px;
      background-color: #f8fafc;
    }}
    
    td {{
      padding: 14px 16px;
      border-bottom: 1px solid var(--border-color);
      color: #334155;
    }}
    
    tr:hover td {{
      background-color: #f8fafc;
    }}
    
    .badge {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      text-align: center;
    }}
    
    .cat-auto {{ color: var(--color-auto); background-color: var(--bg-auto); }}
    .cat-review {{ color: var(--color-review); background-color: var(--bg-review); }}
    .cat-manual {{ color: var(--color-manual); background-color: var(--bg-manual); }}
    
    .findings-cell {{
      color: var(--text-muted);
      font-size: 12.5px;
    }}
    
    .meta-box {{
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 40px;
      padding-top: 20px;
      border-top: 1px solid var(--border-color);
      text-align: center;
      line-height: 1.8;
    }}
    
    @media (max-width: 768px) {{
      .kpi-grid {{
        grid-template-columns: 1fr;
      }}
      body {{
        padding: 20px 10px;
      }}
      section {{
        padding: 25px 20px;
      }}
    }}
    
    @media print {{
      body {{
        background-color: #ffffff;
        padding: 0;
      }}
      .container {{
        max-width: 100%;
      }}
      header {{
        background: none !important;
        color: #000000 !important;
        border: 1px solid var(--border-color);
        box-shadow: none !important;
        padding: 20px;
      }}
      header .subtitle {{
        color: #334155;
      }}
      .badge-report {{
        border: 1px solid #64748b;
        color: #000000;
      }}
      .kpi-card {{
        border: 1px solid var(--border-color);
        box-shadow: none !important;
      }}
      section {{
        border: 1px solid var(--border-color);
        box-shadow: none !important;
        page-break-inside: avoid;
      }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="badge-report">Pre-migration Assessment</div>
      <h1>Oracle → PostgreSQL 移行 事前診断レポート</h1>
      <div class="subtitle">データベーススキーマおよび SQL 資産の移行容易性分析</div>
    </header>

    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-val">{agg['file_count']}</div>
        <div class="kpi-label">対象 SQL ファイル数</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-val">{agg['total_loc']:,}</div>
        <div class="kpi-label">合計行数（コメント除外）</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-val">{agg['total_effort_pd']}</div>
        <div class="kpi-label">概算工数（人日）</div>
      </div>
    </div>

    <section>
      <h2>変換可否の分布</h2>
      <div class="summary-bar">
        <div class="seg-auto" style="width: {pct(auto_c)}%">AUTO {pct(auto_c)}%</div>
        <div class="seg-review" style="width: {pct(review_c)}%">REVIEW {pct(review_c)}%</div>
        <div class="seg-manual" style="width: {pct(manual_c)}%">MANUAL {pct(manual_c)}%</div>
      </div>
      
      <table>
        <thead>
          <tr>
            <th>カテゴリ</th>
            <th>説明</th>
            <th>ファイル数</th>
            <th>構成比</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><span class="badge cat-auto">AUTO</span></td>
            <td>自動変換でほぼ完了。レビュー軽微。</td>
            <td>{auto_c}</td>
            <td>{pct(auto_c)}%</td>
          </tr>
          <tr>
            <td><span class="badge cat-review">REVIEW</span></td>
            <td>構文変換は可能だが、意味の確認・テスト必須。</td>
            <td>{review_c}</td>
            <td>{pct(review_c)}%</td>
          </tr>
          <tr>
            <td><span class="badge cat-manual">MANUAL</span></td>
            <td>PACKAGE / DBMS_* / 設計レベルの作り直しが必要。</td>
            <td>{manual_c}</td>
            <td>{pct(manual_c)}%</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>未対応構文 Top 10（移行を阻む主因）</h2>
      <table>
        <thead>
          <tr>
            <th style="width: 60px;">#</th>
            <th>構文・関数</th>
            <th>検出件数</th>
          </tr>
        </thead>
        <tbody>
          {top_rows or '<tr><td colspan="3">該当なし</td></tr>'}
        </tbody>
      </table>
    </section>

    <section>
      <h2>難易度の高いファイル Top 20（工数集中ポイント）</h2>
      <table>
        <thead>
          <tr>
            <th>ファイル</th>
            <th style="width: 80px;">LOC</th>
            <th style="width: 100px;">カテゴリ</th>
            <th style="width: 100px;">概算工数</th>
            <th>主な検出</th>
          </tr>
        </thead>
        <tbody>
          {detail_rows or '<tr><td colspan="5">該当なし</td></tr>'}
        </tbody>
      </table>
    </section>

    <div class="meta-box">
      ⚠️ 本レポートは <strong>静的解析による事前見積もりの叩き台</strong> です。<br>
      COPY / DB Link / ライブラリ拡張・DDL の解析は含まれていません。<br>
      最終工数は人手レビューで確定し、テスト工数を別途加算してください。<br>
      生成: {datetime.now().isoformat(timespec='seconds')} / Oracle2PostgreSQL diagnose.py
    </div>
  </div>
</body>
</html>"""
    out_path.write_text(body, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Oracle SQL/PLSQL を事前診断して移行可能性レポートを出力",
    )
    parser.add_argument("input_dir", help="走査対象のディレクトリ")
    parser.add_argument("-o", "--output", default="report.csv", help="CSV 出力先（デフォルト: report.csv）")
    parser.add_argument("--html", default=None, help="HTML サマリ出力先（任意）")
    args = parser.parse_args(argv)

    root = Path(args.input_dir).resolve()
    if not root.is_dir():
        print(f"エラー: ディレクトリではありません: {root}", file=sys.stderr)
        return 2

    files = collect_files(root)
    if not files:
        print(f"対象ファイル (.sql) が見つかりません: {root}", file=sys.stderr)
        return 1

    print(f"走査中: {len(files)} ファイル...", file=sys.stderr)
    diags = [diagnose_file(p, root) for p in files]

    out_csv = Path(args.output).resolve()
    write_csv(diags, out_csv)
    print(f"✓ CSV: {out_csv}", file=sys.stderr)

    if args.html:
        out_html = Path(args.html).resolve()
        write_html(diags, out_html)
        print(f"✓ HTML: {out_html}", file=sys.stderr)

    agg = aggregate(diags)
    by = agg["by_category"]
    print("\n=== 集計 ===", file=sys.stderr)
    print(f"  AUTO   : {by.get('AUTO', 0)} ファイル", file=sys.stderr)
    print(f"  REVIEW : {by.get('REVIEW', 0)} ファイル", file=sys.stderr)
    print(f"  MANUAL : {by.get('MANUAL', 0)} ファイル", file=sys.stderr)
    print(f"  概算工数: {agg['total_effort_hr']} 時間 (≒ {agg['total_effort_pd']} 人日)", file=sys.stderr)
    print("\n  未対応構文 Top 5:", file=sys.stderr)
    for name, count in agg["top_unsupported"][:5]:
        print(f"    - {name}: {count}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
