# Streamiz Clear Example

週次メンテなどで Streamiz のテーブルキャッシュ（RocksDB）をクリアする最小例です。

## 実行
```bash
dotnet run --project examples/streamiz-clear/StreamizClear.csproj
```

出力（例）
```
[streamiz-clear] Clearing Streamiz caches (RocksDB delete=true)...
[streamiz-clear] Done. Next ToListAsync()/Pull will rebuild on demand.
```

本番では `deleteStateDirs: false` での運用を推奨（物理削除は検証/CI向け）。

