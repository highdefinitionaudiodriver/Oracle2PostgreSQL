# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- README に「これは何？（30秒で）」「想定ユースケース・価格帯」セクションを追加（移行アセスメント案件の判断材料）
- SECURITY.md を追加（脆弱性報告フロー）
- 商用利用・カスタマイズ依頼の連絡先を README 末尾に明記

## [0.1.0]

### Added
- Oracle SQL / PL/SQL → PostgreSQL 変換ツール初版
- AST ベースの構文解析（80+ 変換ルール）
- AUTO / REVIEW / MANUAL の重要度別レポート出力（HTML / CSV）
- GUI（Tkinter）・CLI・Docker の 3 モード
- 59 言語 i18n 対応
- YAML 設定ファイルによるルール ON/OFF
- GitHub Actions CI パイプライン
