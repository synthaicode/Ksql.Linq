# Testing Workflow

このドキュメントは、テスト関連作業（ユニットテスト・Integration テスト・物理テスト）の標準的なワークフローをまとめたものです。

## 対象と目的
- 対象:
  - ユニットテスト追加・修正
  - Integration テストの実行・拡張
  - 物理テスト（Kafka / ksqlDB / Schema Registry を含む）実行
- 目的: テストの粒度と実行ポリシーを整理し、誰がどのテストをどのように行うかを明確にする

## 役割
- 迅人: ユニットテスト生成・カバレッジ監視
- 詩音: 物理テスト設計と物理環境テスト実行
- 凪: Kafka / ksqlDB / SR / ネットワークなど環境側の調整
- 鏡花: テスト観点・品質基準の確認
- 楠木: テスト実行結果の記録

## 標準フロー（ユニットテスト）
1. 変更点の特定
   - Code / Design Change Workflow で決まった変更点を起点にテスト対象を決める

2. テストケース設計
   - 迅人が正常系・異常系・境界値など必要な観点を洗い出す

3. 実装と実行
   - テストコードを追加または更新し、対象プロジェクトのテストを実行

4. カバレッジ確認
   - 可能な範囲でカバレッジを確認し、重大な抜けがないことを確認

## 標準フロー（物理テスト）
1. 環境準備
   - 凪が Kafka / ksqlDB / SR を含む物理環境を起動

2. テスト実行
   - 詩音が物理テスト用のテストプロジェクト（例: `Kafka.Ksql.Linq.Tests.Integration`）を実行

3. 失敗時の対応
   - 問題が発生した場合は、関連コンテナの停止・クリーンアップ・再起動を行い、同じテストを再実行
   - 再実行後も問題が再現する場合は、ログを収集し進捗ログへエスカレーション

4. 記録
   - 楠木がテスト結果と主要な気づきを進捗ログや必要に応じて diff_log に記録

## Examples 実行ワークフロー

- 役割
  - 凪: Kafka / ksqlDB / Schema Registry / SR の起動・確認
  - 鳴瀬: examples の restore/build と代表サンプル実行
  - 楠木: 実行結果の progress_log 記録

- 手順
   1. 環境起動（凪）
      - physicalTests 配下で物理テストに使用している YAML（例: `physicalTests/.../*.yml`）を用いて、Kafka / ksqlDB / Schema Registry / SR を起動する。
      - 物理テスト用の起動手順と同じ方法でヘルスチェックを行い、正常起動を確認してから examples 実行に進む。
      メモ: この環境起動手順は、Windows 開発環境で PowerShell から `physicalTests/reset.ps1` を実行することを前提としています。
  2. restore / build（鳴瀬）
     - `Get-ChildItem examples -Recurse -Filter *.csproj | Where-Object { $_.Name -notlike '*Tests.csproj' }` を対象に
       - `dotnet restore <proj>`
       - `dotnet build <proj> --no-restore`
  3. 代表サンプル実行（鳴瀬）
     - 少なくとも次を `dotnet run --no-build` で実行し、動作を確認する:
       - Quick Start 相当サンプル
       - `examples/custom-executor/CustomExecutor.csproj`
  4. レポート（楠木）
     - 対象バージョン、restore/build 成否、代表サンプルの実行結果と気づきを progress_log に箇条書きで記録する。
