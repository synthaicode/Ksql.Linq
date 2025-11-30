Design-time KSQL & Avro Tool (with AI Support)
===========================================

This is a .NET global tool that generates KSQL scripts and Avro schemas
from a Ksql.Linq-based application, without requiring Kafka/ksqlDB/Schema
Registry to be running.<br/> <b>It also supports AI-assisted workflows for developers using Ksql.Linq.</b>

This tool targets Ksql.Linq v0.9.5 or later,

Install
-------

```bash
dotnet tool install --global Ksql.Linq.Cli --version 1.0.0
```

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
- Generate or refine KSQL scripts produced by `dotnet ksql script`.
- Get prompt patterns and anti-patterns specific to Ksql.Linq so the AI respects your model and conventions.

Open `AI_ASSISTANT_GUIDE.md` in your editor and share it with your AI assistant to get better, Ksql.Linq-aware suggestions. 
