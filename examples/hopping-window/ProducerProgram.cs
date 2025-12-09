using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace HoppingWindowExample;

/// <summary>
/// Transaction producer that sends test data to Kafka
/// </summary>
class ProducerProgram
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Transaction Producer ===\n");
        Console.WriteLine("This producer generates sample transactions and sends them to Kafka.");
        Console.WriteLine("Press Ctrl+C to stop.\n");

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
            Console.WriteLine("\nShutting down gracefully...");
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9093",
            ClientId = "transaction-producer",
            // Enable idempotence for exactly-once semantics
            EnableIdempotence = true,
            // Retry settings
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,
            // Compression
            CompressionType = CompressionType.Snappy
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:18081",
            // Timeout settings for Schema Registry
            RequestTimeoutMs = 30000,
            MaxCachedSchemas = 100
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        var avroSerializerConfig = new AvroSerializerConfig
        {
            // Automatically register schemas if they don't exist
            AutoRegisterSchemas = true,
            // Use topic name strategy for subject naming
            SubjectNameStrategy = SubjectNameStrategy.TopicRecord
        };

        using var producer = new ProducerBuilder<string, TransactionAvro>(producerConfig)
            .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, new AvroSerializerConfig
            {
                AutoRegisterSchemas = true
            }))
            .SetValueSerializer(new AvroSerializer<TransactionAvro>(schemaRegistry, avroSerializerConfig))
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            .Build();

        var random = new Random();
        var users = new[] { "user_A", "user_B", "user_C", "user_D", "user_E" };
        var currencies = new[] { "USD", "EUR", "JPY", "GBP", "CAD" };

        Console.WriteLine("Starting to produce transactions...\n");

        var transactionId = 1;
        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    // Generate random transaction
                    var userId = users[random.Next(users.Length)];
                    var amount = Math.Round(random.NextDouble() * 500 + 10, 2); // $10 - $510
                    var currency = currencies[random.Next(currencies.Length)];

                    var transaction = new TransactionAvro
                    {
                        transaction_id = $"txn_{transactionId:D6}",
                        user_id = userId,
                        amount = amount,
                        currency = currency,
                        transaction_time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };

                    var result = await producer.ProduceAsync(
                        "transactions",
                        new Message<string, TransactionAvro>
                        {
                            Key = transaction.transaction_id,
                            Value = transaction
                        },
                        cts.Token);

                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Produced: {transaction.transaction_id} | " +
                                    $"User: {userId} | Amount: ${amount:F2} {currency} | " +
                                    $"Offset: {result.Offset}");

                    transactionId++;

                    // Wait between 500ms to 2 seconds before next transaction
                    await Task.Delay(random.Next(500, 2000), cts.Token);
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancellation is requested
                    break;
                }
                catch (ProduceException<string, TransactionAvro> ex)
                {
                    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
                    if (!cts.Token.IsCancellationRequested)
                    {
                        await Task.Delay(1000, cts.Token);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error producing transaction: {ex.Message}");
                    if (!cts.Token.IsCancellationRequested)
                    {
                        await Task.Delay(1000, cts.Token);
                    }
                }
            }
        }
        finally
        {
            // Flush any remaining messages before shutting down
            Console.WriteLine("\nFlushing remaining messages...");
            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"Producer stopped. Total transactions sent: {transactionId - 1}");
        }
    }
}

/// <summary>
/// Avro schema for Transaction
/// Matches the schema registered in Schema Registry
/// </summary>
public class TransactionAvro
{
    public string transaction_id { get; set; } = string.Empty;
    public string user_id { get; set; } = string.Empty;
    public double amount { get; set; }
    public string currency { get; set; } = string.Empty;
    public long transaction_time { get; set; }
}
