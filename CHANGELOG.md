# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- README に「これは何？（30秒で）」「想定ユースケース・価格帯」セクションを追加（移行アセスメント案件の判断材料）
- SECURITY.md を追加（脆弱性報告フロー）
- 商用利用・カスタマイズ依頼の連絡先を README 末尾に明記
- examples/sample_oracle_app/README.md — 診断デモ用サンプルと期待レポート例
- **scripts/diagnose.py** — 既存変換パイプラインに依存しない事前診断スクリプト（営業・見積もり用）
  - 入力ディレクトリの SQL を AUTO / REVIEW / MANUAL に分類
  - 未対応構文 Top 10 ランキング
  - 概算工数（時間／人日）
  - HTML サマリ出力（A4 印刷で 1〜2 枚に収まる形）
  - UTF-8 BOM 付き CSV（Excel での文字化け回避）
  - SQL コメント / 文字列リテラル内のキーワードは除外して誤検出回避

## [0.1.0]

### Added
- Oracle SQL / PL/SQL → PostgreSQL 変換ツール初版
- AST ベースの構文解析（80+ 変換ルール）
- AUTO / REVIEW / MANUAL の重要度別レポート出力（HTML / CSV）
- GUI（Tkinter）・CLI・Docker の 3 モード
- 59 言語 i18n 対応
- YAML 設定ファイルによるルール ON/OFF
- GitHub Actions CI パイプライン
