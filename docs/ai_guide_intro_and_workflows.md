# Ksql.Linq - AI Assistant Guide

> **This document is primarily designed for AI assistants**
> to support high-quality design consultations for Ksql.Linq.
> Human developers may refer to [Design Patterns](#design-patterns), [Decision Trees](#decision-trees), and [Best Practices](#best-practices) sections for quick reference.

**Quick Navigation**:
[For Developers](#-for-developers-how-to-use-this-guide-with-your-ai-assistant) |
[AI Profile](#-ksqllinq-design-support-ai-profile) |
[Design Patterns](#design-patterns) |
[Use Cases](#common-use-cases) |
[API Reference](#api-reference-quick-start) |
[Best Practices](#best-practices)

---

## 👨‍💻 For Developers: How to Use This Guide with Your AI Assistant

This guide is designed to be read by **AI coding assistants** (like Claude, Cursor, GitHub Copilot, ChatGPT, etc.) to provide expert design support for your Ksql.Linq projects.

> **Implementation status note (for both AI and humans)**  
> This guide is intended to describe **currently implemented capabilities** of Ksql.Linq for the given version.  
> When in doubt, AI assistants should:
> - Confirm the caller's **Ksql.Linq version** and deployment environment.
> - Treat any behavior mentioned only in roadmap / design documents as **not yet available** unless the user explicitly confirms otherwise.
> - Prefer concrete, released APIs and patterns from the **API Reference**, **Examples**, and versioned docs under `docs/`.
>
> **Source of truth**  
> In case of any conflict between this guide and the **Ksql.Linq Wiki**, treat the Wiki as the canonical source for actual behavior, configuration, and APIs:  
> https://github.com/synthaicode/Ksql.Linq/wiki

### Where is This File?

This file is included in the **Ksql.Linq** NuGet package, and is also exposed via the CLI. You can access it via:

- **GitHub**: https://github.com/synthaicode/Ksql.Linq/blob/main/AI_ASSISTANT_GUIDE.md
- **NuGet Cache** (Windows): `%userprofile%\.nuget\packages\ksql.linq\<version>\AI_ASSISTANT_GUIDE.md`
- **NuGet Cache** (macOS/Linux): `~/.nuget/packages/ksql.linq/<version>/AI_ASSISTANT_GUIDE.md`
- **Visual Studio/Rider**: Right-click package -> "Open Folder in File Explorer" -> look for `AI_ASSISTANT_GUIDE.md`
- **CLI**: `dotnet ksql ai-assist` prints this guide to standard output; `dotnet ksql ai-assist --copy` also copies the text to the clipboard (where supported).

### Quick Start: Share This Document with Your AI

When starting a new Ksql.Linq project or seeking design advice, give your AI assistant one of these prompts:

**If your AI can access URLs:**
```
Please read https://github.com/synthaicode/Ksql.Linq/blob/main/AI_ASSISTANT_GUIDE.md
and act as a Ksql.Linq Design Support AI.
Help me design my Kafka/ksqlDB stream processing solution following the AI Profile in that document.
```

**If your AI needs a local file path:**
```
I'm working on a Kafka/ksqlDB stream processing project using Ksql.Linq.
Please read the file at <path>/AI_ASSISTANT_GUIDE.md and act as a Ksql.Linq Design Support AI.
Follow the AI Profile guidelines in that document to help me design my solution.
```
(Replace `<path>` with the actual file location from above)

**If your AI cannot access files directly but you have the CLI installed:**
```
dotnet ksql ai-assist --copy
```
Then paste the copied text into your AI assistant and ask it to act as a Ksql.Linq design support AI.

### Recommended Workflows

#### 1. **Initial Design Consultation**

```
I need to design a stream processing solution for [describe your use case].
Please review AI_ASSISTANT_GUIDE.md and help me:
1. Identify the right design patterns
2. Choose between STREAM vs TABLE
3. Decide on Push vs Pull queries
4. Recommend error handling strategies
```

**Expected Output**: The AI will follow the 6-step conversation flow (Prerequisites → Summary → Principles → Options → Recommendation → Next Steps).

---

#### 2. **Architecture Review**

```
I've drafted this Ksql.Linq context [paste your code].
Please review it against the best practices in AI_ASSISTANT_GUIDE.md and suggest improvements.
```

**Expected Output**: Analysis based on Best Practices section, with specific recommendations.

---

#### 3. **Decision Support**

```
I'm deciding between [Option A] and [Option B] for [specific feature].
Based on AI_ASSISTANT_GUIDE.md Decision Trees, which approach do you recommend?
```

**Expected Output**: Trade-off analysis with references to relevant patterns and examples.

---

#### 4. **Implementation Guidance**

```
I want to implement [specific pattern, e.g., windowed aggregation].
Please show me the Design Pattern from AI_ASSISTANT_GUIDE.md and adapt it to my scenario.
```

**Expected Output**: Code example based on the most relevant design pattern with explanation.

---

### What Your AI Assistant Will Provide

After reading this document, your AI will:

✅ **Ask clarifying questions** before proposing solutions
✅ **Present multiple options** with pros/cons analysis
✅ **Reference specific patterns** from this guide (e.g., "This matches Pattern 7")
✅ **Provide structured responses** using the Output Format template
✅ **Explain trade-offs** clearly and honestly
✅ **Identify open questions** and next steps

---

## License for This Guide

The **Ksql.Linq library code** is licensed under the MIT License.  
The **text of this AI Assistant Guide (including the split `ai_guide_*` documents)** is licensed under **Creative Commons Attribution 4.0 International (CC BY 4.0)**.  
You may copy, adapt, and share the guide as long as you provide appropriate credit to the Ksql.Linq project.

### Integration with Development Tools

#### Cursor AI / VS Code

1. Open `AI_ASSISTANT_GUIDE.md` in your editor
2. Use `@AI_ASSISTANT_GUIDE.md` in Cursor chat to reference it
3. Ask: *"Based on this guide, help me design [your use case]"*

#### GitHub Copilot Chat

1. Reference the file: `#file:AI_ASSISTANT_GUIDE.md`
2. Ask design questions with context

#### ChatGPT / Claude Desktop

1. Upload `AI_ASSISTANT_GUIDE.md` to the conversation
2. Request: *"Act as the Ksql.Linq Design Support AI described in this document"*

#### Any environment with the CLI installed

1. Run `dotnet ksql ai-assist --copy` to copy this guide to your clipboard
2. Paste the text into your AI assistant
3. Ask it to act as the Ksql.Linq Design Support AI and start with prerequisites / summary

---

### Key Sections for Your AI to Reference

| When You Need... | AI Should Read... |
|------------------|-------------------|
| Quick API lookup | [API Reference Quick Start](#api-reference-quick-start) |
| Pattern examples | [Design Patterns](#design-patterns) |
| Real-world scenarios | [Common Use Cases](#common-use-cases) |
| Decision guidance | [Decision Trees](#decision-trees) |
| Production tips | [Best Practices](#best-practices) |
| Examples index | [Examples Index](#examples-index) |

---

### Example Interaction

**You**: "I need to enrich order events with customer data from a table."

**AI** (after reading this doc):

> ## 1. Prerequisites Summary
> - **Pattern**: Stream-Table Join (matches Use Case 2)
> - **Entities**: Order (STREAM), Customer (TABLE)
> - **Query Type**: Push (continuous enrichment)
>
> ## 2. Requirement Summary
> Join order stream with customer table to enrich orders with customer name and tier.
>
> ## 3. Design Principles
> 1. **Correctness**: Ensure join key matches
> 2. **Performance**: Use KsqlTable for materialized customer data
> 3. **Simplicity**: Leverage built-in LINQ `.Join()`
>
> ## 4. Options Analysis
>
> ### Option A: Stream-Table Join (Recommended)
> - **Approach**: Use `.Join()` with `[KsqlTable]` on Customer entity
> - **Pros**: Low latency, automatic table materialization
> - **Cons**: Customer table must fit in memory
> - **When to use**: Customer count < 1M, frequent updates
>
> ### Option B: Stream-Stream Join with Window
> - **Approach**: Window-based join if Customer is also a stream
> - **Pros**: Works with high-volume customer updates
> - **Cons**: More complex, requires windowing
> - **When to use**: Customer updates are event-driven
>
> ## 5. Recommended Solution
> **Option A** - See [Pattern 2: Stream Enrichment](#use-case-2-stream-enrichment-join)
>
> ```csharp
