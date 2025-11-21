"""
ChatGPT API Client
Handles API calls to OpenAI for response generation
"""

from openai import OpenAI
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class ChatGPTClient:
    """OpenAI ChatGPT API client"""

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o",
        max_tokens: int = 2000,
        temperature: float = 0.7
    ):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        logger.info(f"ChatGPT client initialized with model: {model}")

    def generate_response(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None
    ) -> Optional[str]:
        """
        Generate response using ChatGPT API

        Args:
            messages: List of message dicts with role and content
            temperature: Override default temperature
            max_tokens: Override default max_tokens

        Returns:
            Generated response text, or None if failed
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature or self.temperature,
                max_tokens=max_tokens or self.max_tokens,
                top_p=1.0,
                frequency_penalty=0.0,
                presence_penalty=0.0
            )

            content = response.choices[0].message.content
            usage = response.usage

            logger.info(
                f"ChatGPT response generated - "
                f"Input: {usage.prompt_tokens} tokens, "
                f"Output: {usage.completion_tokens} tokens, "
                f"Total: {usage.total_tokens} tokens"
            )

            return content

        except Exception as e:
            logger.error(f"ChatGPT API error: {e}")
            return None

    def estimate_cost(self, prompt_tokens: int, completion_tokens: int) -> float:
        """
        Estimate API cost based on model and tokens

        Args:
            prompt_tokens: Input token count
            completion_tokens: Output token count

        Returns:
            Estimated cost in USD
        """
        # Pricing as of 2024 (update as needed)
        pricing = {
            "gpt-4o": {
                "input": 2.50 / 1_000_000,    # $2.50 per 1M input tokens
                "output": 10.00 / 1_000_000   # $10.00 per 1M output tokens
            },
            "gpt-4-turbo": {
                "input": 10.00 / 1_000_000,
                "output": 30.00 / 1_000_000
            },
            "gpt-3.5-turbo": {
                "input": 0.50 / 1_000_000,
                "output": 1.50 / 1_000_000
            }
        }

        model_pricing = pricing.get(self.model, pricing["gpt-4o"])
        cost = (
            prompt_tokens * model_pricing["input"] +
            completion_tokens * model_pricing["output"]
        )

        return cost

    def test_connection(self) -> bool:
        """
        Test API connection

        Returns:
            True if successful
        """
        try:
            test_messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say 'OK' if you can hear me."}
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=test_messages,
                max_tokens=10
            )

            logger.info("ChatGPT API connection test successful")
            return True

        except Exception as e:
            logger.error(f"ChatGPT API connection test failed: {e}")
            return False
