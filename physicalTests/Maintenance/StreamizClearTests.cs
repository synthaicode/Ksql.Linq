using System;
using System.Threading.Tasks;
using Ksql.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ksql.Linq.Tests.Integration.Maintenance;

public class StreamizClearTests
{
    private sealed class TestContext : KsqlContext
    {
        public TestContext(IConfiguration configuration, ILoggerFactory? lf = null) : base(configuration, lf) { }
        protected override void OnModelCreating(Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder)
        {
            // no entities needed for this smoke test
        }
        protected override bool SkipSchemaRegistration => true; // avoid external SR calls in this smoke test
    }

    private static TestContext CreateContext()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
        return new TestContext(config, NullLoggerFactory.Instance);
    }

    [Fact]
    public async Task ClearStreamizState_Idempotent_NoThrow()
    {
        await using var ctx = CreateContext();
        ctx.ClearStreamizState(deleteStateDirs: false);
        ctx.ClearStreamizState(deleteStateDirs: false);
        await Task.CompletedTask;
    }
}