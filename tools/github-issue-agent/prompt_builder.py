"""
Prompt builder for ChatGPT API
Constructs system and user prompts with context
"""

from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class PromptBuilder:
    """Builds prompts for ChatGPT API"""

    SYSTEM_PROMPT = """You are a technical support assistant for the Ksql.Linq project.

## Project Overview
Ksql.Linq is a C# library that provides a LINQ-style DSL for type-safe Kafka/ksqlDB operations.

Key features:
- LINQ-based DSL for Kafka and ksqlDB operations
- Type-safe schemas with Avro and Schema Registry
- Automatic detection of Streams/Tables and Pull/Push modes
- DLQ (Dead Letter Queue), retry, and commit helpers
- Self-healing persistent queries with automatic stabilization
- Market-schedule-aware OHLC bars for financial data

## Your Role
Answer technical questions about Ksql.Linq with:
1. Accurate technical information
2. Clear code examples when relevant
3. Links to documentation
4. Friendly and constructive tone
5. Japanese or English based on the question language

## Response Format
Structure your responses as:

### Understanding
[Brief summary of the issue/question]

### Solution/Answer
[Detailed explanation with technical accuracy]

### Code Example (if applicable)
```csharp
// Clear, working code examples
```

### References
- [Relevant documentation links]
- [Related issues if applicable]

## Guidelines
- Be precise and technically accurate
- Provide runnable code examples when possible
- If you're unsure, acknowledge limitations
- Suggest alternatives when appropriate
- Be helpful and encouraging
"""

    def __init__(self, readme_content: Optional[str] = None):
        self.readme_content = readme_content or ""

    def build_user_prompt(
        self,
        issue_title: str,
        issue_body: str,
        comments: Optional[List[Dict]] = None,
        related_code: Optional[List[Dict]] = None,
        recent_issues: Optional[List[Dict]] = None
    ) -> str:
        """
        Build user prompt with issue context

        Args:
            issue_title: Issue title
            issue_body: Issue body/description
            comments: List of comments
            related_code: Related code snippets
            recent_issues: Recently resolved similar issues

        Returns:
            Formatted user prompt
        """
        prompt_parts = []

        # Issue information
        prompt_parts.append("## GitHub Issue")
        prompt_parts.append(f"**Title:** {issue_title}")
        prompt_parts.append(f"\n**Description:**\n{issue_body}")

        # Comments
        if comments:
            prompt_parts.append("\n## Discussion")
            for i, comment in enumerate(comments, 1):
                user = comment.get("user", "unknown")
                body = comment.get("body", "")
                prompt_parts.append(f"\n**Comment {i}** (by {user}):\n{body}")

        # Related code
        if related_code:
            prompt_parts.append("\n## Related Code")
            for code in related_code:
                path = code.get("path", "unknown")
                url = code.get("url", "")
                prompt_parts.append(f"- `{path}` - {url}")

        # Recent similar issues
        if recent_issues:
            prompt_parts.append("\n## Similar Resolved Issues")
            for issue in recent_issues:
                number = issue.get("number", "?")
                title = issue.get("title", "")
                url = issue.get("url", "")
                prompt_parts.append(f"- #{number}: {title} - {url}")

        # Task
        prompt_parts.append("\n## Task")
        prompt_parts.append(
            "Please provide a comprehensive and helpful response to this issue. "
            "Use the same language as the issue (Japanese or English)."
        )

        return "\n".join(prompt_parts)

    def build_messages(
        self,
        issue_title: str,
        issue_body: str,
        comments: Optional[List[Dict]] = None,
        related_code: Optional[List[Dict]] = None,
        recent_issues: Optional[List[Dict]] = None
    ) -> List[Dict[str, str]]:
        """
        Build messages array for ChatGPT API

        Returns:
            List of message dicts with role and content
        """
        system_prompt = self.SYSTEM_PROMPT

        # Append README context if available
        if self.readme_content:
            system_prompt += f"\n\n## Project README\n{self.readme_content[:3000]}"

        user_prompt = self.build_user_prompt(
            issue_title=issue_title,
            issue_body=issue_body,
            comments=comments,
            related_code=related_code,
            recent_issues=recent_issues
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        logger.debug(f"Built prompt with {len(messages)} messages")
        return messages

    def estimate_token_count(self, text: str) -> int:
        """
        Rough estimation of token count
        (Real tokenization requires tiktoken library)
        """
        # Rough estimate: 1 token ≈ 4 characters
        return len(text) // 4

    def truncate_if_needed(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = 8000
    ) -> List[Dict[str, str]]:
        """
        Truncate messages if they exceed token limit

        Args:
            messages: Message list
            max_tokens: Maximum token count

        Returns:
            Truncated messages
        """
        total_tokens = sum(
            self.estimate_token_count(msg["content"])
            for msg in messages
        )

        if total_tokens <= max_tokens:
            return messages

        logger.warning(f"Messages exceed {max_tokens} tokens, truncating...")

        # Keep system prompt, truncate user prompt
        system_msg = messages[0]
        user_msg = messages[1]

        # Calculate how much to keep
        system_tokens = self.estimate_token_count(system_msg["content"])
        available_tokens = max_tokens - system_tokens - 100  # buffer

        # Truncate user content
        user_content = user_msg["content"]
        chars_to_keep = available_tokens * 4
        truncated_content = user_content[:chars_to_keep] + "\n\n[... content truncated ...]"

        return [
            system_msg,
            {"role": "user", "content": truncated_content}
        ]
