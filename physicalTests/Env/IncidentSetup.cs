using Ksql.Linq.Incidents;
using System.Runtime.CompilerServices;

namespace Ksql.Linq.Tests.Integration;

internal static class IncidentSetup
{
    [ModuleInitializer]
    internal static void Init()
    {
        // Bind a default sink for incident observation during physical tests
        IncidentBus.SetSink(new LoggerIncidentSink());
    }
}

