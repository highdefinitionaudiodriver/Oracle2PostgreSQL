# scripts/ — 補助ツール

メイン変換パイプライン（`main.py`）に依存せず実行できる、見積もり・診断用の
補助スクリプト集です。

---

## `diagnose.py` — 事前診断レポート（営業・見積もり用）

入力ディレクトリの `.sql` を走査し、**変換せず** に以下を出力します：

- 各ファイルの **AUTO / REVIEW / MANUAL** 分類
- 未対応構文ランキング Top 10
- 概算工数（時間／人日）
- A4 印刷で 1〜2 枚に収まる **HTML サマリ**

> 💡 これは何の役に立つのか：
> 「Oracle→PostgreSQL 移行は本当にできるのか？何人月かかるのか？」を、
> 上司・顧客に **1 ページの数字** で示すための初期見積もり材料です。

### 使い方

```bash
# CSV のみ
python scripts/diagnose.py /path/to/oracle_sql

# HTML サマリも生成（議事録・提案書に貼れる）
python scripts/diagnose.py /path/to/oracle_sql --html report.html

# 出力先指定
python scripts/diagnose.py /path/to/oracle_sql -o estimate.csv
```

実行後、stderr に集計が出ます：

```
走査中: 142 ファイル...
✓ CSV: /work/estimate.csv

=== 集計 ===
  AUTO   : 89 ファイル
  REVIEW : 38 ファイル
  MANUAL : 15 ファイル
  概算工数: 47.5 時間 (≒ 5.9 人日)

  未対応構文 Top 5:
    - PACKAGE → Schema + 個別関数への再設計: 7
    - (+) 旧式外部結合 → LEFT/RIGHT JOIN への書換要: 23
    - DBMS_LOB → PostgreSQL の lo_* 関数や TEXT/BYTEA 操作: 4
    - ROWNUM → ROW_NUMBER() OVER() 等への書換要: 11
    - DECODE(...) → CASE WHEN（パラメータ数で展開）: 18
```

### 判定ルール

スクリプトは以下の 3 段階で分類し、軸別の重み（推定分数）で工数を概算します：

| カテゴリ | 含むもの | 推定工数 |
|---|---|---|
| **AUTO** | VARCHAR2 / NUMBER / NVL / SYSDATE / DUAL / NEXTVAL / `MINUS` / DBMS_OUTPUT 等 | 1 分/件 |
| **REVIEW** | `(+)` 外部結合 / DECODE / ROWNUM / CONNECT BY / TO_DATE / MERGE / `:NEW`/`:OLD` / EXECUTE IMMEDIATE | 2–4 分/件 |
| **MANUAL** | PACKAGE / 自律トランザクション / BULK COLLECT / DBMS_LOB / DBMS_SQL / UTL_FILE / SYNONYM / IOT / Partitioning | 3–12 分/件 |

ファイル単位の分類：
- MANUAL が 1 つでもあれば **MANUAL** 扱い
- REVIEW がある（MANUAL 無し）なら **REVIEW**
- それ以外は **AUTO**

### 出力 CSV のスキーマ

```
relative_path, loc, category,
auto_hits, review_hits, manual_hits,
estimated_effort_minutes, top_findings
```

- 文字コードは **UTF-8 BOM 付き**（Excel での文字化けを回避）
- 改行は CRLF

### 注意事項

- 静的解析のみ。実際の依存（COPY、DB Link、ストアド連鎖呼び出し）は評価しません
- 文字コードは UTF-8 / SJIS / EUC-JP / CP932 / Latin-1 を順に試行
- SQL コメント（`--` / `/* */`）は除外して解析
- 文字列リテラル中のキーワードは検出対象外（誤検出回避）
- 最終工数は **必ず人手レビュー＋テスト工数加算** で確定してください

### 商用利用

- 個人・社内 PoC は無料（MIT）
- 自社資産の **診断レポート受託（A4 PDF 納品 + 推奨移行計画）** は応相談
- 連絡先: highdefinitionaudiodriver@gmail.com
