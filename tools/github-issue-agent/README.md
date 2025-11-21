# GitHub Issue Agent for Windows

自宅PC (Windows) で動作するGitHub Issue自動応答エージェント

## 概要

このエージェントは以下を実行します：
1. GitHub Issueを定期的に監視
2. 新規Issue/コメントを検出
3. ChatGPT APIで回答案を生成
4. Draft保存またはGitHubに自動投稿

## システム要件

- Windows 10/11
- Python 3.9以上
- PowerShell 5.1以上
- インターネット接続

## セットアップ手順

### 1. Python環境準備

```powershell
# Pythonバージョン確認
python --version

# 仮想環境作成（推奨）
python -m venv venv
.\venv\Scripts\Activate.ps1

# 依存関係インストール
pip install -r requirements.txt
```

### 2. 環境変数設定

```powershell
# .envファイル作成
Copy-Item .env.example .env

# エディタで編集
notepad .env
```

必要なAPIキー：
- `GITHUB_TOKEN`: GitHub Personal Access Token
  - Scopes: `repo`, `read:org`
  - 取得方法: https://github.com/settings/tokens
- `OPENAI_API_KEY`: OpenAI APIキー
  - 取得方法: https://platform.openai.com/api-keys

### 3. 設定ファイル編集

```powershell
notepad config.yaml
```

主要設定項目：
- `github.repository`: 監視するリポジトリ
- `github.poll_interval_minutes`: ポーリング間隔（分）
- `chatgpt.model`: 使用するモデル（gpt-4o, gpt-4-turbo等）
- `agent.mode`: 動作モード（draft / auto-post）

### 4. 手動テスト実行

```powershell
# Draft モードでテスト
python agent.py --mode draft

# 特定Issueに対して実行
python agent.py respond --issue 123
```

### 5. タスクスケジューラー登録（自動実行）

```powershell
# 管理者権限でPowerShellを起動
# タスクスケジューラー登録スクリプト実行
.\install-task-scheduler.ps1
```

これにより10分ごとに自動実行されます。

## 使い方

### Draft モード（推奨）

```powershell
python agent.py --mode draft
```

- 回答案を `./drafts/{issue-number}.md` に保存
- Windows通知でレビュー依頼
- 手動確認後、承認コマンドで投稿:
  ```powershell
  python agent.py post-draft --issue 123
  ```

### Auto-post モード

```powershell
python agent.py --mode auto-post
```

- 自動でGitHubに投稿（要注意！）
- `question` または `help wanted` ラベル付きIssueのみ対象

### 手動実行モード

```powershell
# 特定Issue番号を指定
python agent.py respond --issue 123

# 最新Issue 5件をチェック
python agent.py check --latest 5
```

## ディレクトリ構成

```
tools/github-issue-agent/
├── README.md                    # このファイル
├── requirements.txt             # Python依存関係
├── .env.example                 # 環境変数テンプレート
├── .env                         # 実際の環境変数（git無視）
├── config.yaml                  # 設定ファイル
│
├── agent.py                     # メインエージェント
├── github_client.py             # GitHub APIクライアント
├── chatgpt_client.py            # ChatGPT APIクライアント
├── prompt_builder.py            # プロンプト生成
├── response_formatter.py        # 回答フォーマット
├── storage.py                   # 状態管理
│
├── setup.ps1                    # セットアップスクリプト
├── run-agent.ps1                # 実行用スクリプト
├── install-task-scheduler.ps1   # タスクスケジューラー登録
│
├── data/                        # 実行時データ
│   ├── agent_state.json        # エージェント状態
│   └── logs/                   # ログファイル
│
└── drafts/                      # 生成された回答案
    └── issue-{number}.md
```

## トラブルシューティング

### PowerShell実行ポリシーエラー

```powershell
# 実行ポリシーを一時的に変更
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 通知が表示されない

Windows通知を有効化：
1. 設定 → システム → 通知とアクション
2. PowerShellからの通知を許可

### API制限エラー

- GitHub API: 5000リクエスト/時間
- OpenAI API: モデルによる（gpt-4: 10,000 TPM）
- `config.yaml` の `max_daily_responses` を調整

## ログ確認

```powershell
# 最新ログ表示
Get-Content .\data\logs\agent.log -Tail 50

# リアルタイムログ監視
Get-Content .\data\logs\agent.log -Wait
```

## アンインストール

```powershell
# タスクスケジューラー削除
Unregister-ScheduledTask -TaskName "GitHubIssueAgent" -Confirm:$false

# ファイル削除
Remove-Item -Recurse -Force .\tools\github-issue-agent
```

## セキュリティ注意事項

- `.env` ファイルは絶対にGitにコミットしない
- GitHub TokenとOpenAI APIキーは厳重に管理
- auto-postモードは慎重に使用
- 定期的にログをレビュー

## コスト見積もり

### OpenAI API コスト（参考）

- gpt-4o: $2.50 / 1M input tokens, $10.00 / 1M output tokens
- gpt-4-turbo: $10.00 / 1M input tokens, $30.00 / 1M output tokens

1回答あたりの概算コスト:
- 入力: 約3,000トークン（Issue内容 + コンテキスト）
- 出力: 約1,000トークン（回答）
- gpt-4o使用時: 約 $0.017/回答
- 月間100回答: 約 $1.70

## ライセンス

MIT License - Ksql.Linqプロジェクトに準拠
