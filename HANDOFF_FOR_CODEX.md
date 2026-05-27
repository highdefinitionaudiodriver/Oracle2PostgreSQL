# Oracle2PostgreSQL 引き継ぎメモ

## 📊 今回実施した改善（2026-05-27）

- **HTML事前診断レポートのビジュアルデザイン刷新**:
  - `scripts/diagnose.py` の `--html` 出力オプションで生成される事前診断レポート（HTML）のデザインを大幅に刷新。
  - **タイポグラフィの向上**: Google Fonts から `Inter` および `Noto Sans JP` を読み込み適用。
  - **モダンな配色**: 青ベース（`#1e40af`）のカラーパレットを基調に、品位のあるトーンに統一。
  - **KPIカードの視覚化**: 対象ファイル数、合計LOC、概算工数をシャドウ付きカードで整理。
  - **ステータスバッジの改善**: `AUTO` (緑), `REVIEW` (橙), `MANUAL` (赤) を視認性の高いパステルカラーバッジに修正。
  - **印刷（A4 / PDF）対応**: `@media print` CSS を適用し、上司や顧客への提出やPDF保存時にレイアウトが崩れないように最適化。

## 🔍 動作確認

- `samples` ディレクトリを対象にスキャンを行い、エラーなしでCSVおよびHTMLレポートが出力されることを検証済み。
  ```powershell
  python scripts/diagnose.py samples --html report.html
  ```
- 生成されたレポートの構文崩れ、f-stringのパース不具合がないことを確認済み。

## 📦 同期状況

- ブランチ `feat/sellable-v1` にコミット・プッシュ済み。
- Google Drive同期ディレクトリ (`G:\マイドライブ\claudecode\Oracle2PostgreSQL`) へ robocopy で同期完了。
