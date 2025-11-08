
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Ksql.Linq.Tests")]
[assembly: InternalsVisibleTo("Ksql.Linq.Cache.Tests")]
[assembly: InternalsVisibleTo("Ksql.Linq.Tests.Integration")]
// Backward-compat for existing test assembly names
[assembly: InternalsVisibleTo("Kafka.Ksql.Linq.Tests")]
[assembly: InternalsVisibleTo("Kafka.Ksql.Linq.Cache.Tests")]
[assembly: InternalsVisibleTo("Kafka.Ksql.Linq.Tests.Integration")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]