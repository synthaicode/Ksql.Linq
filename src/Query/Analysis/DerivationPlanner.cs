using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Ksql.Linq.Query.Analysis;

internal static class DerivationPlanner
{
    public static IReadOnlyList<DerivedEntity> Plan(TumblingQao qao, EntityModel model)
    {
        var entities = new List<DerivedEntity>();

        var keyShapes = qao.Keys.Select(k =>
        {
            var match = qao.PocoShape.FirstOrDefault(p => p.Name == k)
                ?? throw new InvalidOperationException($"Key property '{k}' not found");
            return match;
        }).ToArray();
        var valueShapes = qao.PocoShape.ToArray();

        var basedOn = qao.BasedOn;

        var topicAttr = model.EntityType.GetCustomAttribute<KsqlTopicAttribute>();
        var baseId = ModelNaming.GetBaseId(model);
        var windows = qao.Windows
            .OrderBy(w => w.Unit switch
            {
                "s" => w.Value / 60m,
                "m" => w.Value,
                "h" => w.Value * 60,
                "d" => w.Value * 1440,
                "wk" => w.Value * 10080,
                "mo" => w.Value * 43200m,
                _ => w.Value
            })
            .ToList();
        if (!windows.Any(w => w.Unit == "s" && w.Value == 1))
            windows.Insert(0, new Timeframe(1, "s"));
        var graceMap = new Dictionary<string, int>();
        // Policy: adopt provided grace as-is. Do not auto-increment per timeframe.
        // Default base is 1s when unspecified.
        var baseGrace = qao.GraceSeconds ?? 1;
        foreach (var tf in windows)
        {
            var key = $"{tf.Value}{tf.Unit}";
            var g = baseGrace;
            if (qao.GracePerTimeframe.TryGetValue(key, out var perTf))
                g = perTf;
            graceMap[key] = g;
        }
        qao.GracePerTimeframe.Clear();
        foreach (var kv in graceMap)
            qao.GracePerTimeframe[kv.Key] = kv.Value;
        // 1s hub は常に作成する。HB/Final/Fill/Prev は生成しない簡素方針。
        var hub = $"{baseId}_1s_rows";
        foreach (var tf in windows)
        {
            var tfStr = $"{tf.Value}{tf.Unit}";
            var liveId = $"{baseId}_{tfStr}_live";
            if (tf.Unit == "s" && tf.Value == 1)
            {
                // 1s hub は常に作成する（WhenEmpty でも）
                var final1sStream = new DerivedEntity
                {
                    Id = hub,
                    Role = Role.Final1sStream,
                    Timeframe = tf,
                    KeyShape = keyShapes,
                    ValueShape = valueShapes,
                    InputHint = null,
                    TimeKey = qao.TimeKey,
                    BasedOnSpec = basedOn,
                    WeekAnchor = qao.WeekAnchor,
                    GraceSeconds = graceMap[tfStr]
                };
                entities.Add(final1sStream);
                continue;
            }

            var live = new DerivedEntity
            {
                Id = liveId,
                Role = Role.Live,
                Timeframe = tf,
                KeyShape = keyShapes,
                ValueShape = valueShapes,
                // Live は常に hub(_1s_rows) を参照する
                InputHint = hub,
                TimeKey = qao.TimeKey,
                BasedOnSpec = basedOn,
                WeekAnchor = qao.WeekAnchor,
                GraceSeconds = graceMap[tfStr]
            };
            entities.Add(live);
        }
        return entities;
    }
}
