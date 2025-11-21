using Confluent.Kafka;
using System.Collections.Generic;
using System.ComponentModel;

namespace Ksql.Linq.Configuration;
public class CommonSection
{
    public CommonSection() { }
    /// <summary>
    /// Kafka broker addresses (comma separated)
    /// </summary>
    [DefaultValue("localhost:9092")]
    public string BootstrapServers { get; init; } = "localhost:9092";

    /// <summary>
    /// Client ID
    /// </summary>
    [DefaultValue("ksql-dsl-client")]
    public string ClientId { get; init; } = "ksql-dsl-client";

    /// <summary>
    /// Kafka Streams application identifier
    /// </summary>
    [DefaultValue("ksql-dsl-app")]
    public string ApplicationId { get; init; } = "ksql-dsl-app";

    /// <summary>
    /// Request timeout (ms)
    /// </summary>
    [DefaultValue(30000)]
    public int RequestTimeoutMs { get; init; } = 30000;

    /// <summary>
    /// Metadata max age (ms)
    /// </summary>
    [DefaultValue(300000)]
    public int MetadataMaxAgeMs { get; init; } = 300000;

    /// <summary>
    /// Security protocol
    /// </summary>
    [DefaultValue(SecurityProtocol.Plaintext)]
    public SecurityProtocol SecurityProtocol { get; init; } = SecurityProtocol.Plaintext;

    /// <summary>
    /// SASL mechanism
    /// </summary>
    public SaslMechanism? SaslMechanism { get; init; }

    /// <summary>
    /// SASL user name
    /// </summary>
    public string? SaslUsername { get; init; }

    /// <summary>
    /// SASL password
    /// </summary>
    public string? SaslPassword { get; init; }

    /// <summary>
    /// SSL CA certificate location
    /// </summary>
    public string? SslCaLocation { get; init; }

    /// <summary>
    /// SSL certificate location
    /// </summary>
    public string? SslCertificateLocation { get; init; }

    /// <summary>
    /// SSL key location
    /// </summary>
    public string? SslKeyLocation { get; init; }

    /// <summary>
    /// SSL key password
    /// </summary>
    public string? SslKeyPassword { get; init; }

    /// <summary>
    /// Additional configuration properties
    /// </summary>
    [DefaultValue(typeof(Dictionary<string, string>))]
    public Dictionary<string, string> AdditionalProperties { get; init; } = new();
}
