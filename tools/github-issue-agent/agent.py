"""
GitHub Issue Prompt Generator
Monitors GitHub issues and generates prompts for ChatGPT web UI
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
from prompt_builder import PromptBuilder
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
    """GitHub Issue Prompt Generator"""

    def __init__(self, config_path: str = "config.yaml"):
        # Load environment variables
        load_dotenv()

        # Load configuration
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        # Initialize GitHub client
        self.github = GitHubClient(
            token=os.getenv('GITHUB_TOKEN'),
            owner=os.getenv('REPOSITORY_OWNER'),
            repo=os.getenv('REPOSITORY_NAME')
        )

        # Load README for context
        readme_content = None
        if self.config['context']['include_readme']:
            readme_content = self.github.get_readme_content()

        self.prompt_builder = PromptBuilder(readme_content=readme_content)

        # Initialize storage
        self.storage = AgentStorage(
            state_file=self.config['storage']['state_file']
        )

        # Ensure directories exist
        Path(self.config['prompts']['output_directory']).mkdir(parents=True, exist_ok=True)
        Path(self.config['storage']['log_directory']).mkdir(parents=True, exist_ok=True)

        logger.info("Issue Prompt Generator initialized")

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

    def process_issue(self, issue: IssueData) -> bool:
        """
        Process a single issue and generate prompt

        Args:
            issue: IssueData object

        Returns:
            True if processed successfully
        """
        logger.info(f"Processing issue #{issue.number}: {issue.title}")

        # Check if already processed
        if self.storage.is_issue_processed(issue.number):
            logger.debug(f"Issue #{issue.number} already processed, skipping")
            return False

        # Check daily limit
        max_prompts = self.config['prompts']['max_daily_prompts']
        if not self.storage.can_respond_today(max_prompts):
            logger.warning(f"Daily prompt limit ({max_prompts}) reached")
            return False

        # Skip bot issues
        if self.config['prompts']['ignore_bot_issues'] and issue.is_bot:
            logger.info(f"Skipping bot issue #{issue.number}")
            return False

        # Get issue discussion
        comments = issue.get_comments()

        # Build prompt
        prompt_text = self.prompt_builder.build_chatgpt_prompt(
            issue_title=issue.title,
            issue_body=issue.body,
            issue_url=issue.url,
            issue_number=issue.number,
            comments=comments
        )

        # Save prompt to file
        self._save_prompt(issue, prompt_text)

        # Update storage
        self.storage.mark_issue_processed(issue.number)
        self.storage.increment_daily_response_count()

        return True

    def _save_prompt(self, issue: IssueData, prompt_text: str):
        """Save prompt to file"""
        output_dir = Path(self.config['prompts']['output_directory'])
        prompt_file = output_dir / f"issue-{issue.number}-prompt.txt"

        # Create header
        header = f"""{'='*70}
GitHub Issue Prompt for ChatGPT
{'='*70}

Issue: #{issue.number}
Title: {issue.title}
URL: {issue.url}
Created: {issue.created_at.strftime('%Y-%m-%d %H:%M:%S')}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'='*70}
INSTRUCTIONS
{'='*70}

1. Copy the prompt below (everything after the separator)
2. Paste it into ChatGPT web UI
3. Review the generated response
4. Post the response to GitHub manually

{'='*70}
PROMPT START
{'='*70}

"""

        footer = f"""

{'='*70}
PROMPT END
{'='*70}

After getting the response from ChatGPT:
- Review for technical accuracy
- Adjust tone if needed
- Post to: {issue.url}
"""

        full_content = header + prompt_text + footer

        prompt_file.write_text(full_content, encoding='utf-8')
        logger.info(f"Prompt saved: {prompt_file}")

        # Notify
        if self.config['notifications']['enabled']:
            self.notify(
                "New Issue Prompt Generated",
                f"Issue #{issue.number}\n{prompt_file}"
            )

    def run_once(self):
        """Run agent once (single check)"""
        logger.info("Running prompt generator...")

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
            if self.process_issue(issue):
                processed_count += 1

        # Update last check
        self.storage.update_last_check()

        logger.info(f"Processing complete: {processed_count}/{len(new_issues)} prompts generated")

        # Show stats
        stats = self.storage.get_stats()
        logger.info(f"Stats: {stats}")

        return processed_count

    def generate_for_issue(self, issue_number: int) -> bool:
        """Manually generate prompt for specific issue"""
        issue = self.github.get_issue(issue_number)
        if not issue:
            logger.error(f"Issue #{issue_number} not found")
            return False

        return self.process_issue(issue)


# CLI Commands

@click.group()
def cli():
    """GitHub Issue Prompt Generator CLI"""
    pass


@cli.command()
def run():
    """Run generator once"""
    agent = IssueAgent()
    agent.run_once()


@cli.command()
@click.option('--issue', type=int, required=True, help='Issue number')
def generate(issue):
    """Generate prompt for specific issue"""
    agent = IssueAgent()
    success = agent.generate_for_issue(issue)
    if success:
        click.echo(f"Successfully generated prompt for issue #{issue}")
    else:
        click.echo(f"Failed to generate prompt for issue #{issue}", err=True)
        sys.exit(1)


@cli.command()
def stats():
    """Show generator statistics"""
    agent = IssueAgent()
    stats = agent.storage.get_stats()
    click.echo("\n=== Generator Statistics ===")
    for key, value in stats.items():
        click.echo(f"{key}: {value}")


@cli.command()
def test():
    """Test GitHub API connection"""
    agent = IssueAgent()

    click.echo("Testing GitHub API...")
    try:
        rate_limit = agent.github.get_rate_limit_info()
        click.echo(f"✓ GitHub API OK - Rate limit: {rate_limit['core']['remaining']}/{rate_limit['core']['limit']}")
    except Exception as e:
        click.echo(f"✗ GitHub API failed: {e}", err=True)
        sys.exit(1)

    click.echo("\n✓ Test passed!")


if __name__ == '__main__':
    cli()
