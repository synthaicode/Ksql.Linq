using System.Threading.Tasks;
using Xunit;
using PhysicalTestEnv;

namespace Ksql.Linq.Tests.Integration;

public class KsqlExclusiveFixture : IAsyncLifetime
{
    private const string KsqlBaseUrl = "http://127.0.0.1:18088";

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync()
    {
        // Do not drop artifacts after tests to allow manual inspection
        return Task.CompletedTask;
    }
}

[CollectionDefinition("KsqlExclusive", DisableParallelization = true)]
public class KsqlExclusiveCollection : ICollectionFixture<KsqlExclusiveFixture>
{
}