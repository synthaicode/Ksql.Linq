"""
GitHub Issue Agent - Main Entry Point
Monitors GitHub issues and generates AI responses
"""

import os
import sys
import logging
import yaml
import click
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List
from dotenv import load_dotenv

# Windows notification support
try:
    import win32api
    import win32con
    WINDOWS_NOTIFICATIONS = True
except ImportError:
    WINDOWS_NOTIFICATIONS = False

from github_client import GitHubClient, IssueData
from chatgpt_client import ChatGPTClient
from prompt_builder import PromptBuilder
from response_formatter import ResponseFormatter
from storage import AgentStorage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('./data/logs/agent.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class IssueAgent:
    """Main GitHub Issue Agent"""

    def __init__(self, config_path: str = "config.yaml"):
        # Load environment variables
        load_dotenv()

        # Load configuration
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        # Initialize components
        self.github = GitHubClient(
            token=os.getenv('GITHUB_TOKEN'),
            owner=os.getenv('REPOSITORY_OWNER'),
            repo=os.getenv('REPOSITORY_NAME')
        )

        self.chatgpt = ChatGPTClient(
            api_key=os.getenv('OPENAI_API_KEY'),
            model=self.config['chatgpt']['model'],
            max_tokens=self.config['chatgpt']['max_tokens'],
            temperature=self.config['chatgpt']['temperature']
        )

        readme_content = None
        if self.config['context']['include_readme']:
            readme_content = self.github.get_readme_content()

        self.prompt_builder = PromptBuilder(readme_content=readme_content)

        self.storage = AgentStorage(
            state_file=self.config['storage']['state_file']
        )

        # Ensure directories exist
        Path(self.config['agent']['draft_directory']).mkdir(parents=True, exist_ok=True)
        Path(self.config['storage']['log_directory']).mkdir(parents=True, exist_ok=True)

        logger.info("Issue Agent initialized")

    def notify(self, title: str, message: str):
        """Send Windows notification"""
        if not self.config['notifications']['enabled']:
            return

        if WINDOWS_NOTIFICATIONS:
            try:
                # Simple Windows MessageBox notification
                win32api.MessageBox(
                    0,
                    message,
                    title,
                    win32con.MB_ICONINFORMATION | win32con.MB_TOPMOST
                )
            except Exception as e:
                logger.warning(f"Failed to send notification: {e}")
        else:
            logger.info(f"NOTIFICATION: {title} - {message}")

    def process_issue(self, issue: IssueData, mode: str = "draft") -> bool:
        """
        Process a single issue

        Args:
            issue: IssueData object
            mode: Processing mode (draft | auto-post)

        Returns:
            True if processed successfully
        """
        logger.info(f"Processing issue #{issue.number}: {issue.title}")

        # Check if already processed
        if self.storage.is_issue_processed(issue.number):
            logger.debug(f"Issue #{issue.number} already processed, skipping")
            return False

        # Check daily limit
        max_responses = self.config['agent']['max_daily_responses']
        if not self.storage.can_respond_today(max_responses):
            logger.warning(f"Daily response limit ({max_responses}) reached")
            return False

        # Skip bot issues
        if self.config['agent']['ignore_bot_issues'] and issue.is_bot:
            logger.info(f"Skipping bot issue #{issue.number}")
            return False

        # Get issue discussion
        comments = issue.get_comments()

        # Build prompt
        messages = self.prompt_builder.build_messages(
            issue_title=issue.title,
            issue_body=issue.body,
            comments=comments
        )

        # Truncate if needed
        max_input_tokens = self.config['chatgpt']['max_input_tokens']
        messages = self.prompt_builder.truncate_if_needed(messages, max_input_tokens)

        # Generate response
        logger.info(f"Generating response for issue #{issue.number}...")
        response_text = self.chatgpt.generate_response(messages)

        if not response_text:
            logger.error(f"Failed to generate response for issue #{issue.number}")
            return False

        # Format response
        formatted_response = ResponseFormatter.format_for_github(
            response_text=response_text,
            issue_number=issue.number
        )

        # Save or post based on mode
        if mode == "draft":
            self._save_draft(issue, formatted_response, response_text)
        elif mode == "auto-post":
            success = self.github.post_comment(issue.number, formatted_response)
            if success:
                logger.info(f"Posted response to issue #{issue.number}")
                self.notify(
                    "Response Posted",
                    f"Auto-posted response to issue #{issue.number}"
                )
            else:
                return False

        # Update storage
        self.storage.mark_issue_processed(issue.number)
        self.storage.increment_daily_response_count()

        return True

    def _save_draft(self, issue: IssueData, formatted_response: str, raw_response: str):
        """Save response as draft file"""
        draft_dir = Path(self.config['agent']['draft_directory'])
        draft_file = draft_dir / f"issue-{issue.number}.md"

        draft_content = ResponseFormatter.format_draft(
            response_text=raw_response,
            issue_number=issue.number,
            issue_title=issue.title,
            issue_url=issue.url
        )

        draft_file.write_text(draft_content, encoding='utf-8')
        logger.info(f"Draft saved: {draft_file}")

        # Notify
        if self.config['notifications']['enabled']:
            self.notify(
                "Draft Generated",
                f"Review draft for issue #{issue.number}\n{draft_file}"
            )

    def post_draft(self, issue_number: int) -> bool:
        """Post a draft to GitHub"""
        draft_file = Path(self.config['agent']['draft_directory']) / f"issue-{issue_number}.md"

        if not draft_file.exists():
            logger.error(f"Draft file not found: {draft_file}")
            return False

        # Read draft content (skip header)
        content = draft_file.read_text(encoding='utf-8')
        lines = content.split('\n')

        # Find response content section
        response_lines = []
        in_response = False
        for line in lines:
            if line.strip() == "## Response Content":
                in_response = True
                continue
            if in_response and line.strip().startswith("---"):
                break
            if in_response:
                response_lines.append(line)

        response_text = '\n'.join(response_lines).strip()

        if not response_text:
            logger.error("No response content found in draft")
            return False

        # Post to GitHub
        success = self.github.post_comment(issue_number, response_text)

        if success:
            logger.info(f"Posted draft to issue #{issue_number}")
            # Archive draft
            archive_file = draft_file.with_suffix('.posted.md')
            draft_file.rename(archive_file)
            return True
        else:
            logger.error(f"Failed to post draft to issue #{issue_number}")
            return False

    def run_once(self, mode: str = "draft"):
        """Run agent once (single check)"""
        logger.info(f"Running agent in {mode} mode...")

        # Get last check time
        last_check = self.storage.get_last_check()
        if not last_check:
            # First run - check recent issues
            last_check = datetime.now() - timedelta(hours=24)

        # Get new issues
        new_issues = self.github.get_new_issues_since(
            since=last_check,
            labels_to_watch=self.config['github'].get('labels_to_watch'),
            labels_to_skip=self.config['github'].get('labels_to_skip')
        )

        logger.info(f"Found {len(new_issues)} new issues to process")

        # Process each issue
        processed_count = 0
        for issue in new_issues:
            if self.process_issue(issue, mode=mode):
                processed_count += 1

        # Update last check
        self.storage.update_last_check()

        logger.info(f"Processing complete: {processed_count}/{len(new_issues)} issues processed")

        # Show stats
        stats = self.storage.get_stats()
        logger.info(f"Stats: {stats}")

        return processed_count

    def respond_to_issue(self, issue_number: int, mode: str = "draft") -> bool:
        """Manually respond to specific issue"""
        issue = self.github.get_issue(issue_number)
        if not issue:
            logger.error(f"Issue #{issue_number} not found")
            return False

        return self.process_issue(issue, mode=mode)


# CLI Commands

@click.group()
def cli():
    """GitHub Issue Agent CLI"""
    pass


@cli.command()
@click.option('--mode', type=click.Choice(['draft', 'auto-post']), default='draft',
              help='Processing mode')
def run(mode):
    """Run agent once"""
    agent = IssueAgent()
    agent.run_once(mode=mode)


@cli.command()
@click.option('--issue', type=int, required=True, help='Issue number')
@click.option('--mode', type=click.Choice(['draft', 'auto-post']), default='draft')
def respond(issue, mode):
    """Respond to specific issue"""
    agent = IssueAgent()
    success = agent.respond_to_issue(issue, mode=mode)
    if success:
        click.echo(f"Successfully processed issue #{issue}")
    else:
        click.echo(f"Failed to process issue #{issue}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--issue', type=int, required=True, help='Issue number')
def post_draft(issue):
    """Post a draft response to GitHub"""
    agent = IssueAgent()
    success = agent.post_draft(issue)
    if success:
        click.echo(f"Posted draft for issue #{issue}")
    else:
        click.echo(f"Failed to post draft for issue #{issue}", err=True)
        sys.exit(1)


@cli.command()
def stats():
    """Show agent statistics"""
    agent = IssueAgent()
    stats = agent.storage.get_stats()
    click.echo("\n=== Agent Statistics ===")
    for key, value in stats.items():
        click.echo(f"{key}: {value}")


@cli.command()
def test():
    """Test API connections"""
    agent = IssueAgent()

    click.echo("Testing GitHub API...")
    try:
        rate_limit = agent.github.get_rate_limit_info()
        click.echo(f"✓ GitHub API OK - Rate limit: {rate_limit['core']['remaining']}/{rate_limit['core']['limit']}")
    except Exception as e:
        click.echo(f"✗ GitHub API failed: {e}", err=True)
        sys.exit(1)

    click.echo("Testing ChatGPT API...")
    if agent.chatgpt.test_connection():
        click.echo("✓ ChatGPT API OK")
    else:
        click.echo("✗ ChatGPT API failed", err=True)
        sys.exit(1)

    click.echo("\n✓ All tests passed!")


if __name__ == '__main__':
    cli()
