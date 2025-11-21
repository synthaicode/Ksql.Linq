# クイックスタートガイド

5分でGitHub Issue Agentを始める

## 前提条件

- Windows 10/11
- Python 3.9以上 ([ダウンロード](https://www.python.org/downloads/))
- GitHub Personal Access Token
- OpenAI API Key

## 1. APIキーの取得

### GitHub Token

1. https://github.com/settings/tokens にアクセス
2. "Generate new token" → "Generate new token (classic)"
3. 必要なスコープ:
   - ✓ `repo` (Full control of private repositories)
   - ✓ `read:org` (Read org and team membership)
4. トークンをコピーして保存

### OpenAI API Key

1. https://platform.openai.com/api-keys にアクセス
2. "Create new secret key" をクリック
3. キーをコピーして保存

## 2. セットアップ (3ステップ)

PowerShellを開いて以下を実行:

```powershell
# 1. ディレクトリに移動
cd path\to\Ksql.Linq\tools\github-issue-agent

# 2. セットアップスクリプト実行
.\setup.ps1

# 3. 環境変数を設定
notepad .env
```

`.env` ファイルに以下を記入:

```ini
GITHUB_TOKEN=ghp_あなたのトークン
REPOSITORY_OWNER=synthaicode
REPOSITORY_NAME=Ksql.Linq
OPENAI_API_KEY=sk-あなたのキー
AGENT_MODE=draft
```

保存して閉じる。

## 3. 動作確認

```powershell
# API接続テスト
python agent.py test

# ✓ GitHub API OK
# ✓ ChatGPT API OK
# と表示されればOK
```

## 4. 試しに実行

```powershell
# Draft モードで実行（安全）
python agent.py run --mode draft

# 新しいIssueがあれば drafts/ に回答案が保存される
```

生成された回答案を確認:

```powershell
# Draftファイルを開く
notepad drafts\issue-123.md

# 問題なければGitHubに投稿
python agent.py post-draft --issue 123
```

## 5. 自動実行の設定（オプション）

10分ごとに自動実行するには:

```powershell
# 管理者権限でPowerShellを起動
# タスクスケジューラーに登録
.\install-task-scheduler.ps1
```

これで完全自動化！

## 使い方

### 手動で特定Issueに回答

```powershell
# Issue #456 に回答案を生成
python agent.py respond --issue 456 --mode draft

# 確認して投稿
python agent.py post-draft --issue 456
```

### 統計情報を見る

```powershell
python agent.py stats
```

### ログを確認

```powershell
# 最新50行を表示
Get-Content data\logs\agent.log -Tail 50

# リアルタイム監視
Get-Content data\logs\agent.log -Wait
```

## 設定のカスタマイズ

`config.yaml` を編集:

```yaml
agent:
  mode: "draft"  # draft | auto-post
  max_daily_responses: 20  # 1日の最大回答数

chatgpt:
  model: "gpt-4o"  # gpt-4o | gpt-4-turbo | gpt-3.5-turbo
  temperature: 0.7

github:
  poll_interval_minutes: 10
  labels_to_watch:
    - "question"
    - "help wanted"
```

## トラブルシューティング

### PowerShell実行ポリシーエラー

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### モジュールが見つからない

```powershell
# 仮想環境を有効化
.\venv\Scripts\Activate.ps1

# 再インストール
pip install -r requirements.txt
```

### 通知が表示されない

Windowsの設定で通知を有効化:
- 設定 → システム → 通知とアクション
- PowerShellからの通知を許可

## 安全な使い方

1. **最初はdraftモードで始める**
   - auto-postはテスト後に
2. **定期的に回答をレビュー**
   - AIは間違えることもある
3. **max_daily_responsesを設定**
   - コスト管理
4. **ログを確認**
   - 問題の早期発見

## コスト目安

- **gpt-4o 使用時**: 約 $0.017/回答
- **月間100回答**: 約 $1.70

詳細は `README.md` を参照。

## さらに詳しく

- 完全なドキュメント: [README.md](README.md)
- 設定オプション: [config.yaml](config.yaml)
- ログ: `data/logs/agent.log`

## サポート

問題が発生した場合:
1. `python agent.py test` で接続確認
2. `data/logs/agent.log` でエラーログ確認
3. Issue作成: https://github.com/synthaicode/Ksql.Linq/issues
