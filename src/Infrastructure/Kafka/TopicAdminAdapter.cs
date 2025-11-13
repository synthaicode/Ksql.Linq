using Ksql.Linq.Infrastructure.Admin;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Kafka;

internal sealed class TopicAdminAdapter : ITopicAdmin
{
    private readonly KafkaAdminService _inner;

    public TopicAdminAdapter(KafkaAdminService inner)
    {
        _inner = inner;
    }

    public Task EnsureDlqTopicExistsAsync(CancellationToken ct = default)
        => _inner.EnsureDlqTopicExistsAsync(ct);

    public Task EnsureCompactedTopicAsync(string topicName, CancellationToken ct = default)
        => _inner.EnsureCompactedTopicAsync(topicName, ct);

    public void ValidateKafkaConnectivity()
        => _inner.ValidateKafkaConnectivity();
}
