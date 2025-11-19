# CLI Tool Workflow (Ksql.Linq.Cli)

## 1. Purpose

This document describes how to build, test, and publish the `Ksql.Linq.Cli` .NET tool that exposes:

- `dotnet ksql script` – design-time KSQL script generation
- `dotnet ksql avro` – design-time Avro schema (`.avsc`) generation

The goal is to make the tool easy to build locally and to publish via CI (e.g., to nuget.org or GitHub Packages).

---

## 2. Project Layout

- Library (core):
  - `src/Ksql.Linq.csproj`
  - Contains `KsqlContext`, `IDesignTimeKsqlContextFactory`, `DefaultKsqlScriptBuilder`, `DefaultAvroSchemaExporter`, etc.
- CLI tool:
  - `src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj`
  - Contains:
    - `Program` (entry point)
    - `Commands/ScriptCommand` – for KSQL script generation
    - `Commands/AvroCommand` – for Avro schema generation
    - `Services/AssemblyResolver` – resolves `.csproj` / `.dll` to an assembly path
    - `Services/DesignTimeContextLoader` – loads `KsqlContext` via `IDesignTimeKsqlContextFactory`

Key `csproj` settings for CLI:

- `PackAsTool=true`
- `ToolCommandName=dotnet-ksql`
- `PackageId=Ksql.Linq.Cli`
- `TargetFramework=net8.0`

---

## 3. Local Build & Pack Workflow

### 3.1 Build library and CLI

```bash
dotnet build src/Ksql.Linq.csproj -c Release
dotnet build src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release
```

### 3.2 Pack CLI as .NET tool package

```bash
dotnet pack src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj \
  -c Release \
  -o ./artifacts/cli
```

- Output: `./artifacts/cli/Ksql.Linq.Cli.<version>.nupkg`

### 3.3 Install as local/global tool (for smoke tests)

Global installation from the local source:

```bash
dotnet tool install --global Ksql.Linq.Cli \
  --add-source ./artifacts/cli
```

Then verify:

```bash
dotnet ksql script --help
dotnet ksql avro --help
```

If you prefer a local tool (per repo):

```bash
dotnet new tool-manifest
dotnet tool install Ksql.Linq.Cli --add-source ./artifacts/cli
```

---

## 4. CLI Commands Overview

### 4.1 `dotnet ksql script`

Generates a design-time KSQL script from a compiled assembly that exposes `IDesignTimeKsqlContextFactory`.

Example:

```bash
dotnet ksql script \
  --project ./src/MyApp/MyApp.csproj \
  --output ./ksql/generated.sql \
  --verbose
```

Options:

- `-p, --project` (required): project file (`.csproj`) or assembly (`.dll`) path.
- `-c, --context`: `KsqlContext` class name (required if multiple factories exist).
- `-o, --output`: output file path for the generated SQL; if omitted, outputs to stdout.
- `--config`: path to an `appsettings.json` passed to the design-time factory (optional).
- `--no-header`: exclude header comments (GeneratedBy/TargetAssembly/GeneratedAt).
- `-v, --verbose`: enable detailed logging (resolved assembly, context type, entity count, etc.).

Under the hood:

- `AssemblyResolver.Resolve(...)` figures out the target DLL from the project/assembly path.
- `DesignTimeContextLoader.Load(...)` finds and invokes the appropriate `IDesignTimeKsqlContextFactory` to obtain a `KsqlContext` instance.
- `DefaultKsqlScriptBuilder.Build(context)` generates a `KsqlScript`, and `ToSql()` produces the final script text.

### 4.2 `dotnet ksql avro`

Generates Avro schema files (`.avsc`) for each entity’s value schema.

Example:

```bash
dotnet ksql avro \
  --project ./src/MyApp/MyApp.csproj \
  --output ./schemas \
  --verbose
```

Options:

- `-p, --project` (required): project file (`.csproj`) or assembly (`.dll`) path.
- `-c, --context`: `KsqlContext` class name (required if multiple factories exist).
- `-o, --output` (required): output directory for `.avsc` files.
- `--config`: `appsettings.json` path (optional).
- `-v, --verbose`: detailed logging.

Under the hood:

- Same DLL/context resolution as `script`.
- Uses `DefaultAvroSchemaExporter.ExportValueSchemas(context)` to obtain `Dictionary<string,string>` (key: entity type name, value: Avro schema JSON).
- Writes one `.avsc` file per entity into the specified output directory.

---

## 5. CI Publishing Flows

This section outlines typical GitHub Actions workflows to build and publish the CLI tool package.  
We support both **GitHub Packages (RC/pre)** and **nuget.org (stable)**, mirroring the main library release flow.

### 5.1 Prerequisites

- GitHub Packages:
  - Use `GITHUB_TOKEN` (or a PAT) with `packages:write` permission.
  - Consumers need a `NuGet.config` that points to the GitHub feed.
- nuget.org:
  - API key stored as `NUGET_API_KEY` in GitHub Secrets.
- Tagging:
  - RC/preview: `cli-v*.*.*-rc.*`
  - Stable: `cli-v*.*.*`

### 5.2 Publish to GitHub Packages (RC / internal)

```yaml
name: Publish Ksql.Linq.Cli Tool (GitHub Packages)

on:
  workflow_dispatch:
  push:
    tags:
      - 'cli-v*.*.*-rc.*'

jobs:
  pack-and-publish:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '8.0.x'

      - name: Restore CLI project
        run: dotnet restore src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj

      - name: Pack CLI tool
        run: dotnet pack src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release -o ./artifacts/cli

      - name: Add GitHub Packages source
        run: |
          dotnet nuget add source \
            --username "${{ github.actor }}" \
            --password "${{ secrets.GITHUB_TOKEN }}" \
            --store-password-in-clear-text \
            --name "github" \
            "https://nuget.pkg.github.com/${{ github.repository_owner }}/index.json"

      - name: Publish to GitHub Packages
        run: |
          dotnet nuget push "./artifacts/cli/Ksql.Linq.Cli.*.nupkg" \
            --source "github" \
            --skip-duplicate
```

### 5.3 Publish to nuget.org (stable)

```yaml
name: Publish Ksql.Linq.Cli Tool (nuget.org)

on:
  workflow_dispatch:
  push:
    tags:
      - 'cli-v*.*.*'

jobs:
  pack-and-publish:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '8.0.x'

      - name: Restore CLI project
        run: dotnet restore src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj

      - name: Pack CLI tool
        run: dotnet pack src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release -o ./artifacts/cli

      - name: Publish to nuget.org
        env:
          NUGET_API_KEY: ${{ secrets.NUGET_API_KEY }}
        run: |
          dotnet nuget push "./artifacts/cli/Ksql.Linq.Cli.*.nupkg" \
            --api-key "$NUGET_API_KEY" \
            --source "https://api.nuget.org/v3/index.json"
```

After this workflow runs successfully, users can install the tool via:

```bash
dotnet tool install --global Ksql.Linq.Cli
```

and then use:

```bash
dotnet ksql script ...
dotnet ksql avro ...
```

---

## 6. Notes and Best Practices

- Keep CLI behavior thin: all KSQL/Avro generation should delegate to library types (`DefaultKsqlScriptBuilder`, `DefaultAvroSchemaExporter`).
- Prefer `IDesignTimeKsqlContextFactory` implementations in application assemblies for context creation, mirroring EF Core’s design-time factory pattern.
- When changing CLI options or behavior, update both this workflow doc and `designtime_ksql_usage_guide_v0_9_5.md` to keep the narrative consistent for users.
