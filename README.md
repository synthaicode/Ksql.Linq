# <img src="docs/assets/ksqllinq-logo.png" alt="LinqKsql" width="100" height="100" style="vertical-align:middle;margin-right:8px;"/> &nbsp;
&nbsp; Ksql.Linq &nbsp;&nbsp;<img src="docs/assets/experimental.png" alt="Experimental"  height="30" style="vertical-align:middle;margin-right:8px;"/>

> LINQ-style C# DSL for type-safe Kafka/ksqlDB operations.

---

## Overview

Ksql.Linq is a C# library that unifies Kafka/ksqlDB and Avro/Schema Registry usage. It lets you control Kafka Streams and ksqlDB in a LINQ style and offers the following capabilities.

- Operate Kafka and ksqlDB through a LINQ-based DSL.
- Design type-safe schemas with Avro and Schema Registry.
- Detect Streams/Tables and Pull/Push modes automatically.
- Support operations with DLQ, retry, and commit helpers.
- **Self-healing persistent queries:** automatically stabilizes CTAS/CSAS queries
  by retrying, pre-creating internal topics, and recovering from transient errors.
- **Market-schedule–aware OHLC bars (support feature):**
   Generate OHLC bars (e.g., 1s/1m/5m/15m/1h) strictly aligned to exchange trading sessions.
   The engine skips closed hours and holidays, handles DST correctly, and offers gap policies
   (skip, carry-forward close, or emit sentinel). Pre-/post-market can be toggled per schedule.
---

## 🚀 Examples
See practical usage examples in the  
👉 [Ksql.Linq Wiki – Examples](https://github.com/synthaicode/Ksql.Linq/wiki/Examples)

---

## 📚 Documentation
Full guides, design notes, and examples are available in the
👉 [Ksql.Linq Wiki](https://github.com/synthaicode/Ksql.Linq/wiki).  

---

## 🧭 License and roadmap

- License: [MIT License](./LICENSE)
- Documentation: portions will adopt [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Planned work (examples):
  - Expand examples
  - Prepare for .NET 10 support

---

## 🤝 Acknowledgements

This library was built under the theme of "AI and human co-creation" with support from the Amagi, Naruse, Shion, Kyouka, Kusunoki, Jinto, Hiromu, and Hazuki AI agents. See [Acknowledgements.md](./docs/acknowledgements.md) for details.

---
