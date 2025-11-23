# Ksql.Linq CLI `--ai-support` Messages

> Authored by **Hiromu** (広夢) – documentation and communication support role.

This document defines the **fixed header and footer messages** that the `dotnet ksql ai-support` command will prepend/append around the `AI_ASSISTANT_GUIDE.md` content.

The guide itself remains **English-only** and may change per version, but these messages should remain stable to keep the UX consistent.

---

## 1. English Header (prepend before guide)

```text
# How to use this text with your AI assistant

Copy this entire output and paste it into your AI assistant, or save it as a file and ask:

"Read this Ksql.Linq AI Assistant Guide and act as a design support AI for my Ksql.Linq project."
```

---

## 2. English Footer (append after guide)

```text
Note: This guide is specific to your installed Ksql.Linq version.
When you upgrade Ksql.Linq, run "dotnet ksql ai-support" again and share the new output with your AI assistant.
```

---

## 3. Optional Japanese Header/Footer (for `ja` environments)

The guide body stays English-only. These Japanese lines are optional and may be used when the CLI detects a Japanese UI culture (e.g., `ja-JP`).

### 3.1 Japanese Header

```text
# このテキストをAIアシスタントで使うには

この出力全体をAIアシスタントに貼り付けるか、ファイルに保存して次のように依頼してください：

「この Ksql.Linq AI Assistant Guide を読んで、私の Ksql.Linq プロジェクトの設計アシスタントとして振る舞ってください。」
```

### 3.2 Japanese Footer

```text
注意: このガイドは、インストールされている Ksql.Linq のバージョンに依存します。
Ksql.Linq をアップグレードしたら、再度 "dotnet ksql ai-support" を実行し、新しい出力をAIアシスタントに共有してください。
```

---

## 4. Implementation Notes (non-normative)

- Detect environment language via `.NET` culture (e.g., `CultureInfo.CurrentUICulture`).
- Default to **English**; optionally switch header/footer to Japanese when the UI culture is Japanese, while keeping the guide body in English.
- Do not localize `AI_ASSISTANT_GUIDE.md` itself; this document is primarily for AI consumption.
