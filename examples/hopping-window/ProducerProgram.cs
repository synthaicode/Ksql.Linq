using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
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

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9093",
            ClientId = "transaction-producer"
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:18081"
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<string, TransactionAvro>(producerConfig)
            .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
            .SetValueSerializer(new AvroSerializer<TransactionAvro>(schemaRegistry))
            .Build();

        var random = new Random();
        var users = new[] { "user_A", "user_B", "user_C", "user_D", "user_E" };
        var currencies = new[] { "USD", "EUR", "JPY", "GBP", "CAD" };

        Console.WriteLine("Starting to produce transactions...\n");

        var transactionId = 1;
        while (true)
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
                    });

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Produced: {transaction.transaction_id} | " +
                                $"User: {userId} | Amount: ${amount:F2} {currency} | " +
                                $"Offset: {result.Offset}");

                transactionId++;

                // Wait between 500ms to 2 seconds before next transaction
                await Task.Delay(random.Next(500, 2000));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error producing transaction: {ex.Message}");
                await Task.Delay(1000);
            }
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
