# CLAUDE.md - AI Assistant Guidelines for Ksql.Linq

This document provides essential context for AI assistants working with the Ksql.Linq codebase.

## Project Overview

Ksql.Linq is a C# library that provides a LINQ-style DSL for type-safe Kafka/ksqlDB operations. It unifies Kafka, ksqlDB, Avro, and Schema Registry usage in a fluent C# API.

**Key Features:**
- LINQ-based DSL for Kafka and ksqlDB operations
- Type-safe schema design with Avro and Schema Registry
- Automatic detection of Streams/Tables and Pull/Push modes
- DLQ (Dead Letter Queue), retry, and commit helpers
- Self-healing persistent queries
- Market-schedule-aware OHLC bars generation

**Tech Stack:**
- .NET 8.0
- Confluent.Kafka 2.12.0
- Apache.Avro 1.12.0
- Streamiz.Kafka.Net 1.7.1
- xUnit + Moq for testing

## Repository Structure

```
Ksql.Linq/
├── src/                    # Main library source
│   ├── Application/        # Application-level components
│   ├── Cache/              # Caching infrastructure
│   ├── Configuration/      # Configuration handling
│   ├── Context/            # KsqlContext implementations
│   ├── Core/               # Core functionality
│   ├── EntitySets/         # Entity set abstractions
│   ├── Events/             # Event handling
│   ├── Extensions/         # Extension methods
│   ├── Infrastructure/     # Infrastructure utilities
│   ├── Ksql.Linq.Cli/      # CLI tool (separate project)
│   ├── Mapping/            # Object mapping
│   ├── Messaging/          # Messaging infrastructure
│   ├── Query/              # Query building and execution
│   ├── Runtime/            # Runtime components
│   ├── SchemaRegistryTools/# Schema Registry utilities
│   ├── SerDes/             # Serialization/Deserialization
│   └── Window/             # Windowing operations
├── tests/                  # Unit tests
├── physicalTests/          # Integration tests (require Docker)
├── examples/               # Example projects (~30+ examples)
├── docs/                   # Documentation
├── features/               # Feature development area
└── .github/workflows/      # CI/CD workflows
```

## Development Commands

### Build
```bash
# Restore dependencies
dotnet restore

# Build in Release mode
dotnet build --configuration Release --no-restore

# Build main library only
dotnet build src/Ksql.Linq.csproj -c Release
```

### Test
```bash
# Run unit tests (excludes integration tests)
dotnet test tests/Ksql.Linq.Tests.csproj --configuration Release --filter "TestCategory!=Integration"

# Run CLI tests
dotnet test tests/Ksql.Linq.Cli.Tests/Ksql.Linq.Cli.Tests.csproj -c Release

# Run with coverage
dotnet test tests/Ksql.Linq.Tests.csproj --configuration Release --collect:"XPlat Code Coverage" --filter "TestCategory!=Integration"
```

### Public API Validation
```bash
# Strict public API check (required for stable releases)
dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017
```

### Physical/Integration Tests
Physical tests require Docker with Kafka, ksqlDB, and Schema Registry:
```powershell
# From physicalTests/ directory
pwsh -NoLogo -File .\reset.ps1   # Reset environment
dotnet test Ksql.Linq.Tests.Integration.csproj -c Release --filter "FullyQualifiedName~TestClassName"
```

## Code Conventions

### C# Standards
- **Nullable reference types:** Enabled (`<Nullable>enable</Nullable>`)
- **Warnings as errors:** Enabled with exceptions for nullable warnings (CS8600-CS8625)
- **Documentation:** XML documentation enabled
- **Target Framework:** net8.0

### Public API Management
- Uses Microsoft.CodeAnalysis.PublicApiAnalyzers
- API surface tracked in `src/PublicAPI.Shipped.txt` and `src/PublicAPI.Unshipped.txt`
- New public APIs must be added to `PublicAPI.Unshipped.txt`
- Strict checks (RS0016/RS0017) enforced only for stable releases

### Testing Conventions
- **Framework:** xUnit 2.8.1
- **Mocking:** Moq 4.20.69
- **Test Categories:** Use `[Trait("TestCategory", "Integration")]` for integration tests
- **InternalsVisibleTo:** Tests can access internal members

### Naming Conventions
- Test files: `{ClassName}Tests.cs`
- Test methods: Descriptive names indicating what is being tested
- Integration tests: Marked with `TestCategory=Integration` trait

## CI/CD Workflows

### Pull Request Validation (`pr-validate.yml`)
- Builds library
- Packs preview package
- Builds all examples against preview package

### Unit Tests (`ci.yml`)
- Runs on PRs to main
- Builds and tests (excluding integration)
- Generates coverage report

### Release Flow

**RC (Release Candidate):**
- Tag format: `v*.*.*-rc.*` (e.g., `v0.9.5-rc1`)
- Publishes to GitHub Packages
- No strict Public API check

**Stable Release:**
- Tag format: `v*.*.*` (e.g., `v0.9.5`)
- Publishes to nuget.org
- Strict Public API check enforced
- Tags must be created on `main` branch

## Key Project Files

- `Ksql.Linq.sln` - Solution file
- `src/Ksql.Linq.csproj` - Main library project
- `global.json` - .NET SDK version (8.0.416)
- `src/PublicAPI.*.txt` - Public API tracking

## Working with the Codebase

### Adding New Features
1. Create feature branch
2. Implement in appropriate `src/` subdirectory
3. Add unit tests in `tests/`
4. Update `PublicAPI.Unshipped.txt` for new public APIs
5. Build with `dotnet build -c Release`
6. Run tests with `dotnet test`

### Modifying Existing APIs
- If changing public API signatures, update `PublicAPI.Unshipped.txt`
- Run strict check locally before PR: `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`

### Integration Testing
- Physical tests require Docker environment
- Always reset environment before running: `pwsh -NoLogo -File physicalTests/reset.ps1`
- Physical tests are Windows-focused; CI runs unit tests only

## Dependencies

### Core Dependencies
- Confluent.Kafka, Confluent.SchemaRegistry
- Apache.Avro
- Streamiz.Kafka.Net (can use local project or NuGet)
- Microsoft.Extensions.* (DI, Configuration, Logging, Options)

### Streamiz Configuration
- Default: Uses NuGet packages (`UseStreamizPackages=true`)
- Development: Can switch to local project references for debugging

## CLI Tool

`Ksql.Linq.Cli` is a .NET global tool for design-time KSQL and Avro generation:

```bash
# Install
dotnet tool install --global Ksql.Linq.Cli

# Generate KSQL script
dotnet ksql script --project ./MyApp.csproj --output ./ksql/generated.sql

# Generate Avro schemas
dotnet ksql avro --project ./MyApp.csproj --output ./schemas
```

## Important Notes

### Test Execution Policy
- Unit tests and CLI tests: Run in all environments
- Integration tests: Windows environment only, require Docker stack
- CI environments (Linux): Unit tests + CLI tests only

### Suppressed Warnings
The project suppresses certain warnings for phased migration:
- Nullable warnings (CS8600-CS8625): Warnings, not errors
- Documentation warnings (CS1570, CS1573, CS1587): Suppressed
- Public API warnings (RS0025, RS0026): Suppressed except in strict mode

### Version Management
- Version defined in `src/Ksql.Linq.csproj` `<Version>` element
- Streamiz versions configurable via MSBuild properties

## Documentation

- Main documentation: [Wiki](https://github.com/synthaicode/Ksql.Linq/wiki)
- Examples guide: [Wiki Examples](https://github.com/synthaicode/Ksql.Linq/wiki/Examples)
- Release process: `docs/release_publish_flow.md`

## AI Team Context

This project uses an AI team collaboration model (see `AGENTS.md`). Key conventions:
- Design changes and diffs go in `docs/diff_log/`
- Feature work organized in `features/{feature-name}/`
- Progress tracking and documentation is important

## Quick Reference

| Task | Command |
|------|---------|
| Build | `dotnet build -c Release` |
| Test (unit) | `dotnet test tests/Ksql.Linq.Tests.csproj -c Release --filter "TestCategory!=Integration"` |
| Pack | `dotnet pack src/Ksql.Linq.csproj -c Release` |
| Public API check | `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017` |
| CLI tests | `dotnet test tests/Ksql.Linq.Cli.Tests/Ksql.Linq.Cli.Tests.csproj -c Release` |
