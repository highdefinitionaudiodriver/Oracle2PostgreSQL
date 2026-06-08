---
title: "Oracle2PostgreSQL - DB移行ツール を作った — ローカル完結で動かす実用ツール"
emoji: "🛠️"
type: "tech"
topics: ["python", "個人開発", "oss"]
published: false
---

> 本記事は Zenn 用の下書きです。Qiita に出す場合は先頭の frontmatter を削除してください。

## TL;DR

Oracle → PostgreSQL 移行前の SQL / PL/SQL 資産診断・変換支援ツール

- リポジトリ: https://github.com/highdefinitionaudiodriver/Oracle2PostgreSQL
- ライセンス: MIT / バージョン: v0.1.0

## 作った背景・課題

（なぜ作ったか。既存ツールの不満、手作業の手間などを 2〜3 段落で。）

## できること

- 80+ 変換ルール — データ型、関数、構文、PL/SQL、トリガー、シーケンス、シノニムを網羅
- AST ベース解析 — CREATE TABLE / INDEX / VIEW / SEQUENCE / SYNONYM / PROCEDURE / FUNCTION / PACKAGE / TRIGGER を構造的にパース
- PL/SQL → PL/pgSQL 変換 — パラメータ、例外処理、カーソル、DBMS_OUTPUT 等を自動変換
- トリガー分離変換 — Oracle トリガーを PostgreSQL の「関数 + CREATE TRIGGER」形式に分離
- パッケージ変換ガイド — PACKAGE を Schema + 個別関数へ変換するガイドコメントを自動挿入
- HTML / CSV 移行レポート — 変更一覧、重要度別集計、ファイル別詳細を出力
- 59 言語 i18n — 日本語・英語・中国語・韓国語・フランス語など 59 言語の GUI / レポート
- YAML 設定ファイル — ルール ON/OFF、スキーママッピング、ログレベルを外部設定化

## 仕組み / 工夫した点

（設計上のポイント。ローカル完結・プライバシー配慮・依存の少なさ など。）

## 使い方

```bash
# インストール・起動例（README から転記）
```

## ハマったところ

（開発中の課題と解決。）

## おわりに

フィードバックは Issues / Star をいただけると励みになります。

リポジトリ: https://github.com/highdefinitionaudiodriver/Oracle2PostgreSQL
