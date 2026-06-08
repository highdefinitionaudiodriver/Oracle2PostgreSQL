# Oracle2PostgreSQL - DB移行ツール v0.1.0

Oracle → PostgreSQL 移行前の SQL / PL/SQL 資産診断・変換支援ツール

## 主な機能

- 80+ 変換ルール — データ型、関数、構文、PL/SQL、トリガー、シーケンス、シノニムを網羅
- AST ベース解析 — CREATE TABLE / INDEX / VIEW / SEQUENCE / SYNONYM / PROCEDURE / FUNCTION / PACKAGE / TRIGGER を構造的にパース
- PL/SQL → PL/pgSQL 変換 — パラメータ、例外処理、カーソル、DBMS_OUTPUT 等を自動変換
- トリガー分離変換 — Oracle トリガーを PostgreSQL の「関数 + CREATE TRIGGER」形式に分離
- パッケージ変換ガイド — PACKAGE を Schema + 個別関数へ変換するガイドコメントを自動挿入
- HTML / CSV 移行レポート — 変更一覧、重要度別集計、ファイル別詳細を出力
- 59 言語 i18n — 日本語・英語・中国語・韓国語・フランス語など 59 言語の GUI / レポート
- YAML 設定ファイル — ルール ON/OFF、スキーママッピング、ログレベルを外部設定化

## 動作環境

- Windows 10/11, macOS 12+, Linux / Python 3.10 以上

## ダウンロード

- `*.zip` … 実行ファイル一式（解凍してそのまま実行）
- ソースコードは下記リポジトリを参照

## ライセンス / 連絡先

- MIT License
- https://github.com/highdefinitionaudiodriver/Oracle2PostgreSQL
- highdefinitionaudiodriver@gmail.com

## 変更履歴

（`CHANGELOG.md` の該当バージョンを転記してください）
