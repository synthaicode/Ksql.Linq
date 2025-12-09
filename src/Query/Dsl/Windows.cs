namespace Ksql.Linq.Query.Dsl;

public class Windows
{
    public int[]? Minutes { get; set; }
    public int[]? Hours { get; set; }
    public int[]? Days { get; set; }
    public int[]? Months { get; set; }
}

public class HoppingWindows
{
    public int Size { get; set; }
    public int AdvanceBy { get; set; }
    public string Unit { get; set; } = "MINUTES";
}
