using Ksql.Linq.Infrastructure.Kafka;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Runtime.Cache;
using Ksql.Linq.Runtime.Dlq;
using Ksql.Linq.Runtime.Monitor;
using Ksql.Linq.Runtime.Schema;
using Ksql.Linq.Runtime.Scheduling;
using Ksql.Linq.Runtime.Fill;

namespace Ksql.Linq.Runtime.Context;

/// <summary>
/// Optional dependency bag for orchestrated KsqlContext lifecycle.
/// Allows progressive migration without breaking existing constructors.
/// </summary>
internal sealed class KsqlContextDependencies
{
    public IKsqlExecutor? KsqlExecutor { get; set; }
    public ISchemaRegistrar? SchemaRegistrar { get; set; }
    public ITopicAdmin? TopicAdmin { get; set; }
    public IDlqService? DlqService { get; set; }
    public IRowMonitorCoordinator? RowMonitorCoordinator { get; set; }
    public IMarketScheduleService? MarketScheduleService { get; set; }
    public ITableCacheManager? TableCacheManager { get; set; }
    public IStartupFillService? StartupFillService { get; set; }
}
