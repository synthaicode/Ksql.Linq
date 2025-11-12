namespace Ksql.Linq.SchemaRegistryTools;

public static class SchemaSubjects
{
    public static string KeyFor(string topic) => $"{topic}-key";
    public static string ValueFor(string topic) => $"{topic}-value";
}

