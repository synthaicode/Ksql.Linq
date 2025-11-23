# AI Assistant Guide ↔ Wiki Mapping

> This document defines how major sections of `AI_ASSISTANT_GUIDE.md` relate to the **canonical** Ksql.Linq Wiki pages.  
> The Wiki is the source of truth for behavior, configuration, and APIs. The AI guide is an AI-oriented summary and conversation scaffold.

---

## 1. High-level Concept Mapping

| AI_ASSISTANT_GUIDE area                          | Canonical Wiki page(s)                                       | Notes |
|--------------------------------------------------|---------------------------------------------------------------|-------|
| Overall product / context                        | `Overview.md`, `Architecture-Overview.md`                     | High-level understanding of Ksql.Linq and its architecture. |
| Streams vs Tables concepts                       | `Streams-and-Tables.md`                                       | Conceptual model and KSQL mapping. |
| Kafka / ksqlDB / Ksql.Linq user journey          | `Kafka-Ksql-Linq-User-Guide.md`, `Quick-Start.md`             | End-to-end flow for new users. |
| Examples index / scenarios                       | `Examples.md`, individual `Examples-*.md`                     | Concrete samples referenced from AI guide. |

---

## 2. API and Configuration

| AI_ASSISTANT_GUIDE area                          | Canonical Wiki page(s)                                       | Notes |
|--------------------------------------------------|---------------------------------------------------------------|-------|
| API Reference Quick Start                        | `API-Reference.md`, `Public-API.md`                          | Public surface and usage details. |
| POCO attributes / entity design                  | `POCO-Attributes.md`, `Constraints-Decimal.md`               | Attribute semantics and constraints. |
| Configuration / appsettings                      | `Configuration-Reference.md`, `Appsettings.md`, `Appsettings-Kafka.md` | Detailed options and defaults. |
| KSQL API and statements                          | `KSQL-API.md`                                                | KSQL-specific behavior and mapping. |

---

## 3. CLI and Design-Time Tools

| AI_ASSISTANT_GUIDE area                          | Canonical Wiki page(s)                                       | Notes |
|--------------------------------------------------|---------------------------------------------------------------|-------|
| Design-time KSQL/Avro generation (CLI)           | `CLI-Usage.md`, `API-Workflow.md`                            | `dotnet ksql script` / `dotnet ksql avro` behavior. |
| Runtime tuning / performance                     | `Runtime-Tuning-Plan-v0-9-6.md`, `Lag-Monitoring-and-Tuning.md` | Throughput, lag, and tuning strategies. |

---

## 4. Runtime Behavior and Operations

| AI_ASSISTANT_GUIDE area                          | Canonical Wiki page(s)                                       | Notes |
|--------------------------------------------------|---------------------------------------------------------------|-------|
| Startup / warmup / monitoring                    | `Operations-Startup-and-Monitoring.md`, `Operations-Startup-Warmup.md`, `Observation-Events.md` | Operational behavior and observability. |
| Produce / consume / DLQ                          | `Produce-Consume-and-DLQ.md`, `Consumption-Rules.md`         | Messaging patterns and error handling. |
| Schema registry usage and policies               | `Schema-Registry-Policy.md`, `Ddl-Topic-Policy.md`           | Schema evolution and topic DDL policies. |

---

## 5. Tumbling / Windowing & Streamiz

| AI_ASSISTANT_GUIDE area                          | Canonical Wiki page(s)                                       | Notes |
|--------------------------------------------------|---------------------------------------------------------------|-------|
| Tumbling / windowing concepts and limitations    | `Tumbling-Overview.md`, `Tumbling-Definition.md`, `Tumbling-Consumption.md`, `Tumbling-Topics-Config.md`, `Tumbling-Fault-Tolerance.md` | Window semantics and pitfalls. |
| Streamiz-specific considerations                 | `Streamiz-Clear.md`                                          | Backend-specific behavior and constraints. |

---

## 6. Maintenance Rules

- When **Wiki content changes** in the pages listed above:
  - Treat the Wiki as authoritative.
  - Review corresponding passages in `AI_ASSISTANT_GUIDE.md` and adjust wording if they diverge.
- When updating the AI guide:
  - Prefer to **link back to the Wiki** for deep details instead of duplicating them.
  - If a new AI guide section has no clear Wiki counterpart, consider whether a new Wiki page should be added.

