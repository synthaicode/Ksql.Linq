using Ksql.Linq.Runtime.Context;
using Ksql.Linq.Runtime.Schema;
using Ksql.Linq.Runtime.Monitor;
using Ksql.Linq.Runtime.Scheduling;
using Ksql.Linq.Runtime.Cache;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Infrastructure.Kafka;
using Ksql.Linq.Runtime.Dlq;
using Ksql.Linq.Runtime.Fill;

namespace Ksql.Linq.Runtime.Context;

/// <summary>
/// Fluent builder to compose optional services for KsqlContext orchestration.
/// This is additive and non-breaking; existing constructors remain valid.
/// </summary>
internal sealed class KsqlContextBuilder
{
    private readonly KsqlContextDependencies _deps = new();

    public KsqlContextBuilder WithKsqlExecutor(IKsqlExecutor executor)
    { _deps.KsqlExecutor = executor; return this; }

    public KsqlContextBuilder WithSchemaRegistrar(ISchemaRegistrar registrar)
    { _deps.SchemaRegistrar = registrar; return this; }

    public KsqlContextBuilder WithTopicAdmin(ITopicAdmin admin)
    { _deps.TopicAdmin = admin; return this; }

    public KsqlContextBuilder WithDlqService(IDlqService dlq)
    { _deps.DlqService = dlq; return this; }

    public KsqlContextBuilder WithRowMonitorCoordinator(IRowMonitorCoordinator coordinator)
    { _deps.RowMonitorCoordinator = coordinator; return this; }

    public KsqlContextBuilder WithMarketScheduleService(IMarketScheduleService svc)
    { _deps.MarketScheduleService = svc; return this; }

    public KsqlContextBuilder WithTableCacheManager(ITableCacheManager cache)
    { _deps.TableCacheManager = cache; return this; }

    public KsqlContextBuilder WithStartupFillService(IStartupFillService svc)
    { _deps.StartupFillService = svc; return this; }

    public KsqlContextDependencies Build() => _deps;

    /// <summary>
    /// Apply the built dependencies to a KsqlContext instance.
    /// </summary>
    public void ApplyTo(KsqlContext context)
    {
        context.ApplyDependencies(_deps);
    }
}
