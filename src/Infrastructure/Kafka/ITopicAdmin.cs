using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Kafka;

/// <summary>
/// Administrative operations for Kafka topics required by the runtime (e.g., DLQ).
/// </summary>
internal interface ITopicAdmin
{
    Task EnsureDlqTopicExistsAsync(CancellationToken ct = default);
    Task EnsureCompactedTopicAsync(string topicName, CancellationToken ct = default);
    void ValidateKafkaConnectivity();
}


