using Ksql.Linq.Core.Attributes;

[KsqlTopic("bar_1m_live")]
public class Bar1mLive
{
    [KsqlKey(0)] public string Broker { get; set; } = string.Empty;
    [KsqlKey(1)] public string Symbol { get; set; } = string.Empty;
    public System.DateTime BucketStart { get; set; }
    public double Open  { get; set; }
    public double High  { get; set; }
    public double Low   { get; set; }
    public double Close { get; set; }
}
