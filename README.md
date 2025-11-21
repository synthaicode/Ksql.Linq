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

## 🤖 AI-Assisted Development

Using AI coding assistants (Cursor, GitHub Copilot, Claude, ChatGPT)? This package includes **AI_ASSISTANT_GUIDE.md** — a comprehensive guide designed for AI agents to provide expert design support for Ksql.Linq projects.

### Where to Find AI_ASSISTANT_GUIDE.md

**Option 1: View on GitHub** (easiest)
- 📄 [AI_ASSISTANT_GUIDE.md on GitHub](https://github.com/synthaicode/Ksql.Linq/blob/main/AI_ASSISTANT_GUIDE.md)

**Option 2: In Your NuGet Package Cache**
- Windows: `%userprofile%\.nuget\packages\ksql.linq\<version>\AI_ASSISTANT_GUIDE.md`
- macOS/Linux: `~/.nuget/packages/ksql.linq/<version>/AI_ASSISTANT_GUIDE.md`

**Option 3: In Visual Studio / Rider**
1. Right-click on `Ksql.Linq` package in Solution Explorer
2. Select "Open Folder in File Explorer" / "Show in Explorer"
3. Look for `AI_ASSISTANT_GUIDE.md` in the package root

**Option 4: Clone the Repository**
```bash
git clone https://github.com/synthaicode/Ksql.Linq.git
# AI_ASSISTANT_GUIDE.md is in the root directory
```

### Quick Start with Your AI Assistant

Give your AI this prompt (replace `<path>` with the actual location):

```
Please read the file at <path>/AI_ASSISTANT_GUIDE.md and act as a Ksql.Linq Design Support AI.
Follow the AI Profile guidelines in that document to help me design my stream processing solution.
```

**Or** (if your AI can fetch from URLs):
```
Please read https://github.com/synthaicode/Ksql.Linq/blob/main/AI_ASSISTANT_GUIDE.md
and help me design my Ksql.Linq stream processing solution.
```

### What Your AI Will Provide

After reading AI_ASSISTANT_GUIDE.md, your AI assistant will:
- ✅ Follow a structured 6-step design consultation flow
- ✅ Present multiple architectural options with pros/cons
- ✅ Reference specific patterns from the library
- ✅ Provide production-ready recommendations
- ✅ Identify open questions and next steps

See AI_ASSISTANT_GUIDE.md for full details on AI-assisted workflows, example interactions, and integration guides for Cursor/Copilot/ChatGPT/Claude.

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


