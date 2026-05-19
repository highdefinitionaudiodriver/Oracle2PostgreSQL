# Sample Oracle Application — 診断デモ用

このディレクトリは **Oracle2PostgreSQL の診断機能を試す**ためのサンプル Oracle SQL 一式です。
中規模業務アプリを想定した、`AUTO` / `REVIEW` / `MANUAL` 3 段階の難易度が混在するコードを意図的に含めています。

---

## 想定シナリオ

> ある中堅企業の社内システム（顧客管理＋受注管理＋在庫管理）が Oracle 11g で動いている。
> Aurora PostgreSQL への移行アセスメントを依頼された SI ベンダーが、
> 「変換可能率」「手修正箇所」「概算工数」を 1 ページの A4 レポートにまとめたい。

---

## ファイル構成

```
sample_oracle_app/
├── README.md                       （このファイル）
├── 01_schema/
│   ├── 01_tables.sql              テーブル定義（NUMBER, VARCHAR2, DATE, CLOB 混在）
│   ├── 02_sequences.sql           シーケンス（.NEXTVAL / .CURRVAL 使用箇所あり）
│   ├── 03_constraints.sql         FK / CHECK / UNIQUE 制約
│   └── 04_indexes.sql             B-Tree / Function-Based / Bitmap インデックス
├── 02_views/
│   ├── 01_customer_summary.sql    INNER JOIN ベース
│   └── 02_sales_report.sql        CONNECT BY 再帰（要 REVIEW）
├── 03_plsql/
│   ├── 01_simple_procedures.sql   AUTO レベル
│   ├── 02_package_body.sql        PACKAGE（MANUAL 必須）
│   ├── 03_triggers.sql            行レベル / 文レベルトリガー
│   └── 04_complex_business.sql    EXCEPTION / CURSOR / DBMS_OUTPUT 混在
└── 04_queries/
    └── 01_reports.sql              DECODE / NVL / SYS_GUID / 外部結合(+)
```

---

## 期待される診断結果（参考値）

このサンプルを `oracle2postgresql --diagnose ./examples/sample_oracle_app` で診断すると、おおむね以下のレポートが出力される想定です：

| カテゴリ | 件数 | 構成比 |
|---|---|---|
| ✅ AUTO（自動変換済み） | 約 60% | データ型・関数の単純置換 |
| ⚠ REVIEW（要レビュー） | 約 25% | (+) 外部結合・CONNECT BY・トリガー |
| ❌ MANUAL（手修正必須） | 約 15% | PACKAGE・複雑な PL/SQL・EXCEPTION |

### 未対応構文 Top 5（参考）

1. `CONNECT BY ... PRIOR`（階層問い合わせ）→ `WITH RECURSIVE` への手動書換
2. `(+)` 旧式外部結合 → `LEFT/RIGHT OUTER JOIN` への手動書換
3. `PACKAGE` → Schema + 個別関数への分解（設計判断必要）
4. `DBMS_*` 組込パッケージ（DBMS_LOB, DBMS_SQL 等）→ PostgreSQL 拡張または手書き
5. `ROWNUM`（特に副問合せ内）→ `ROW_NUMBER() OVER()` への書換

### 概算工数（参考）

- 全体 800 行のうち、AUTO 約 480 行、REVIEW 約 200 行、MANUAL 約 120 行
- 手作業見積もり：**約 5 人日**（テスト含めて 8 人日）

---

## このサンプルをご自身のコードベースに置き換えるには

1. このディレクトリをコピーして自社の Oracle SQL ファイルを配置
2. `oracle2postgresql --diagnose ./your_oracle_code` を実行
3. `report.html` / `report.csv` / `summary.pdf`（diagnose モード）を確認
4. 「未対応構文 Top 10」と「概算工数」を見積もり書のたたき台に

---

## 診断レポート受託のご案内

サンプルではなく **実際のコードベースでの診断レポート（A4 PDF 納品）** をご希望の場合：

- 100 ファイル規模：応相談（数万円〜）
- 1000 ファイル規模：個別見積もり
- 連絡先：highdefinitionaudiodriver@gmail.com

> NDA 締結後にコード受領、診断後はコード一切返却・ローカル削除する運用ポリシーで対応します。
