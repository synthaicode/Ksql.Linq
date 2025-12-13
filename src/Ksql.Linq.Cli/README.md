Design-time KSQL & Avro Tool (with AI Assist)
===========================================

This is a .NET global tool that generates KSQL scripts and Avro schemas
from a Ksql.Linq-based application, without requiring Kafka/ksqlDB/Schema
Registry to be running.

**It also supports AI-assisted workflows for developers using Ksql.Linq.**

This tool is developed alongside Ksql.Linq. For best results, keep the library and CLI versions aligned.

Prerequisites / compatibility
----------------------------

- **.NET SDK**: .NET 8+ (this tool targets `net8.0`).
- **Version pairing (recommended)**: use `Ksql.Linq.Cli 1.1.x` together with `Ksql.Linq 1.1.x`.
  - The `ai-assist` guide is bundled with **Ksql.Linq**; the CLI reads the guide from the library it ships with.
  - Keeping library/CLI versions aligned avoids “guide/content drift” and makes support/debugging simpler.

Install
-------

```bash
dotnet tool install --global Ksql.Linq.Cli
```

Update
------

Global tool:

```bash
dotnet tool update --global Ksql.Linq.Cli
```

Local tool (tool manifest):

```bash
dotnet tool update Ksql.Linq.Cli
```

After updating, rerun `dotnet ksql ai-assist --copy` if you want to refresh the AI Assistant Guide text you paste into your AI assistant.

Commands
--------

1. AI Assist guide

    ```bash
    dotnet ksql ai-assist --copy
    ```
    Paste it into your AI assistant and ask:

        “Read this AI Assistant Guide and assist me in designing or reviewing my Ksql.Linq project.”

1. Generate KSQL script:

    ```bash
    dotnet ksql script \
      --project ./src/MyApp/MyApp.csproj \
      --output ./ksql/generated.sql \
      --verbose
    ```

1. Generate Avro schemas:

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

AI-assisted workflows
---------------------

Ksql.Linq ships with an **AI Assistant Guide** (`AI_ASSISTANT_GUIDE.md`) that explains how to use modern AI coding assistants
(for example: ChatGPT, Claude, GitHub Copilot, Cursor) together with both the library and this CLI tool.

- Ask an AI to design or review your `KsqlContext`, entities, and windowing strategy.
- Explain KSQL scripts produced by `dotnet ksql script` and highlight what to verify on your own ksqlDB version/configuration.
- Get prompt patterns and anti-patterns specific to Ksql.Linq so the AI respects your model and conventions.

Run `dotnet ksql ai-assist --copy`, paste it into your AI assistant, and ask it to follow the guide. 
