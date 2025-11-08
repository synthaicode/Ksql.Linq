using Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;

namespace Ksql.Linq.Cache.Core;

internal class TableCacheRegistry : IDisposable
{
    private readonly Dictionary<Type, object> _caches = new();
    private readonly List<IDisposable> _resources = new();
    private readonly List<string> _stateDirs = new();

    public void Register(Type type, object cache)
    {
        _caches[type] = cache;
    }

    public void RegisterResource(IDisposable disposable)
    {
        if (disposable != null)
            _resources.Add(disposable);
    }

    public void RegisterStateDir(string? path)
    {
        if (!string.IsNullOrWhiteSpace(path))
            _stateDirs.Add(path!);
    }

    // 元の簡易実装（no-op）に戻す
    public void RegisterEligibleTables(IEnumerable<EntityModel> models, HashSet<string> tableTopics)
    {
        // no-op for simplified registry
    }

    public ITableCache<T>? GetCache<T>() where T : class
    {
        return _caches.TryGetValue(typeof(T), out var c) ? (ITableCache<T>)c : null;
    }

    public void Dispose()
    {
        // Dispose external resources (e.g., KafkaStream) first
        foreach (var r in _resources)
        {
            try { r.Dispose(); } catch { }
        }
        _resources.Clear();

        foreach (var c in _caches.Values)
            (c as IDisposable)?.Dispose();
        _caches.Clear();
    }

    public void Clear(bool deleteStateDirs)
    {
        try { Dispose(); } catch { }
        if (deleteStateDirs)
        {
            foreach (var d in _stateDirs)
            {
                try { if (System.IO.Directory.Exists(d)) System.IO.Directory.Delete(d, true); } catch { }
            }
        }
        _stateDirs.Clear();
    }
}
