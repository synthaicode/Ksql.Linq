Ksql.Linq.Cli - design-time KSQL & Avro tool
===========================================

Ksql.Linq.Cli is a .NET global tool that generates KSQL scripts and Avro schemas
from a compiled Ksql.Linq-based application, without requiring Kafka/ksqlDB/Schema
Registry to be running.

This tool targets **Ksql.Linq v0.9.5 or later**.

Install
-------

```bash
dotnet tool install --global Ksql.Linq.Cli
```

Commands
--------

Generate KSQL script:

```bash
dotnet ksql script \
  --project ./src/MyApp/MyApp.csproj \
  --output ./ksql/generated.sql \
  --verbose
```

Generate Avro schemas:

```bash
dotnet ksql avro \
  --project ./src/MyApp/MyApp.csproj \
  --output ./schemas \
  --verbose
```

Both commands expect your assembly to implement `IDesignTimeKsqlContextFactory`,
which creates a design-time `KsqlContext` that configures the model but skips
runtime connections.

Further documentation
---------------------

For more details and options, see the wiki:
https://github.com/synthaicode/Ksql.Linq/wiki/CLI-Usage
