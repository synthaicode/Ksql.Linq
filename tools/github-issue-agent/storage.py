"""
State management for GitHub Issue Agent
Tracks processed issues and daily statistics
"""

import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Set, Optional
import logging

logger = logging.getLogger(__name__)


class AgentStorage:
    """Manages persistent state for the agent"""

    def __init__(self, state_file: str = "./data/agent_state.json"):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        """Load state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return self._default_state()
        return self._default_state()

    def _default_state(self) -> Dict:
        """Default state structure"""
        return {
            "processed_issues": [],
            "processed_comments": [],
            "daily_stats": {},
            "last_check": None,
            "last_response_time": None
        }

    def _save_state(self):
        """Save state to file"""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def is_issue_processed(self, issue_number: int) -> bool:
        """Check if issue has been processed"""
        return issue_number in self.state["processed_issues"]

    def is_comment_processed(self, comment_id: int) -> bool:
        """Check if comment has been processed"""
        return comment_id in self.state["processed_comments"]

    def mark_issue_processed(self, issue_number: int):
        """Mark issue as processed"""
        if issue_number not in self.state["processed_issues"]:
            self.state["processed_issues"].append(issue_number)
            self._save_state()
            logger.info(f"Marked issue #{issue_number} as processed")

    def mark_comment_processed(self, comment_id: int):
        """Mark comment as processed"""
        if comment_id not in self.state["processed_comments"]:
            self.state["processed_comments"].append(comment_id)
            self._save_state()
            logger.debug(f"Marked comment {comment_id} as processed")

    def get_daily_response_count(self) -> int:
        """Get response count for today"""
        today = str(date.today())
        return self.state["daily_stats"].get(today, {}).get("responses", 0)

    def increment_daily_response_count(self):
        """Increment today's response count"""
        today = str(date.today())
        if today not in self.state["daily_stats"]:
            self.state["daily_stats"][today] = {"responses": 0}
        self.state["daily_stats"][today]["responses"] += 1
        self.state["last_response_time"] = datetime.now().isoformat()
        self._save_state()

    def can_respond_today(self, max_responses: int) -> bool:
        """Check if we can respond more today"""
        return self.get_daily_response_count() < max_responses

    def update_last_check(self):
        """Update last check timestamp"""
        self.state["last_check"] = datetime.now().isoformat()
        self._save_state()

    def get_last_check(self) -> Optional[datetime]:
        """Get last check timestamp"""
        if self.state["last_check"]:
            return datetime.fromisoformat(self.state["last_check"])
        return None

    def clean_old_data(self, retention_days: int = 30):
        """Clean old processed items (keep recent only)"""
        # Keep only recent processed issues (last 1000)
        if len(self.state["processed_issues"]) > 1000:
            self.state["processed_issues"] = self.state["processed_issues"][-1000:]

        # Keep only recent processed comments (last 5000)
        if len(self.state["processed_comments"]) > 5000:
            self.state["processed_comments"] = self.state["processed_comments"][-5000:]

        # Clean old daily stats
        cutoff_date = date.today()
        days_to_keep = []
        for day_str in self.state["daily_stats"]:
            try:
                day = date.fromisoformat(day_str)
                if (cutoff_date - day).days <= retention_days:
                    days_to_keep.append(day_str)
            except:
                pass

        self.state["daily_stats"] = {
            day: self.state["daily_stats"][day]
            for day in days_to_keep
        }

        self._save_state()
        logger.info("Cleaned old data from storage")

    def get_stats(self) -> Dict:
        """Get current statistics"""
        return {
            "total_processed_issues": len(self.state["processed_issues"]),
            "total_processed_comments": len(self.state["processed_comments"]),
            "today_responses": self.get_daily_response_count(),
            "last_check": self.get_last_check(),
            "last_response": self.state.get("last_response_time")
        }
