"""
GitHub API Client for Issue Agent
Handles fetching issues, comments, and posting responses
"""

from github import Github, GithubException
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class IssueData:
    """Data class for Issue information"""

    def __init__(self, issue):
        self.number = issue.number
        self.title = issue.title
        self.body = issue.body or ""
        self.state = issue.state
        self.labels = [label.name for label in issue.labels]
        self.created_at = issue.created_at
        self.updated_at = issue.updated_at
        self.user = issue.user.login
        self.is_bot = issue.user.type == "Bot"
        self.url = issue.html_url
        self.comments_count = issue.comments
        self._raw_issue = issue

    def get_comments(self) -> List[Dict]:
        """Get all comments for this issue"""
        try:
            comments = []
            for comment in self._raw_issue.get_comments():
                comments.append({
                    "id": comment.id,
                    "user": comment.user.login,
                    "body": comment.body,
                    "created_at": comment.created_at,
                    "is_bot": comment.user.type == "Bot"
                })
            return comments
        except Exception as e:
            logger.error(f"Failed to fetch comments for issue #{self.number}: {e}")
            return []

    def has_label(self, label: str) -> bool:
        """Check if issue has specific label"""
        return label in self.labels

    def has_any_label(self, labels: List[str]) -> bool:
        """Check if issue has any of the specified labels"""
        return any(label in self.labels for label in labels)

    def __repr__(self):
        return f"IssueData(#{self.number}: {self.title[:50]}...)"


class GitHubClient:
    """GitHub API client for issue monitoring"""

    def __init__(self, token: str, owner: str, repo: str):
        self.gh = Github(token)
        self.repo = self.gh.get_repo(f"{owner}/{repo}")
        self.owner = owner
        self.repo_name = repo
        logger.info(f"GitHub client initialized for {owner}/{repo}")

    def get_recent_issues(
        self,
        count: int = 20,
        state: str = "open",
        labels: Optional[List[str]] = None
    ) -> List[IssueData]:
        """
        Get recent issues from repository

        Args:
            count: Number of issues to fetch
            state: Issue state (open, closed, all)
            labels: Filter by labels (optional)

        Returns:
            List of IssueData objects
        """
        try:
            issues = []
            query_params = {"state": state, "sort": "created", "direction": "desc"}

            if labels:
                query_params["labels"] = labels

            for issue in self.repo.get_issues(**query_params)[:count]:
                # Skip pull requests
                if issue.pull_request:
                    continue
                issues.append(IssueData(issue))

            logger.info(f"Fetched {len(issues)} recent issues")
            return issues

        except GithubException as e:
            logger.error(f"GitHub API error: {e}")
            return []

    def get_issue(self, issue_number: int) -> Optional[IssueData]:
        """Get specific issue by number"""
        try:
            issue = self.repo.get_issue(issue_number)
            return IssueData(issue)
        except GithubException as e:
            logger.error(f"Failed to fetch issue #{issue_number}: {e}")
            return None

    def get_new_issues_since(
        self,
        since: datetime,
        labels_to_watch: Optional[List[str]] = None,
        labels_to_skip: Optional[List[str]] = None
    ) -> List[IssueData]:
        """
        Get new issues created since specific time

        Args:
            since: Datetime to check from
            labels_to_watch: Only include issues with these labels
            labels_to_skip: Skip issues with these labels

        Returns:
            List of new IssueData objects
        """
        try:
            new_issues = []
            for issue in self.repo.get_issues(
                state="open",
                sort="created",
                direction="desc",
                since=since
            ):
                # Skip pull requests
                if issue.pull_request:
                    continue

                issue_data = IssueData(issue)

                # Skip if created before cutoff
                if issue_data.created_at <= since:
                    break

                # Skip if has labels to skip
                if labels_to_skip and issue_data.has_any_label(labels_to_skip):
                    continue

                # Filter by watch labels if specified
                if labels_to_watch and not issue_data.has_any_label(labels_to_watch):
                    continue

                new_issues.append(issue_data)

            logger.info(f"Found {len(new_issues)} new issues since {since}")
            return new_issues

        except GithubException as e:
            logger.error(f"Failed to fetch new issues: {e}")
            return []

    def post_comment(self, issue_number: int, comment_body: str) -> bool:
        """
        Post comment to issue

        Args:
            issue_number: Issue number
            comment_body: Comment text (markdown)

        Returns:
            True if successful
        """
        try:
            issue = self.repo.get_issue(issue_number)
            issue.create_comment(comment_body)
            logger.info(f"Posted comment to issue #{issue_number}")
            return True
        except GithubException as e:
            logger.error(f"Failed to post comment to issue #{issue_number}: {e}")
            return False

    def get_readme_content(self) -> str:
        """Fetch README.md content"""
        try:
            readme = self.repo.get_readme()
            content = readme.decoded_content.decode('utf-8')
            logger.debug("Fetched README content")
            return content
        except Exception as e:
            logger.warning(f"Failed to fetch README: {e}")
            return ""

    def search_code(self, query: str, max_results: int = 5) -> List[Dict]:
        """
        Search code in repository

        Args:
            query: Search query
            max_results: Maximum results to return

        Returns:
            List of code snippets with metadata
        """
        try:
            results = []
            search_query = f"{query} repo:{self.owner}/{self.repo_name}"
            code_results = self.gh.search_code(search_query)

            for result in list(code_results)[:max_results]:
                results.append({
                    "path": result.path,
                    "url": result.html_url,
                    "repository": result.repository.full_name
                })

            logger.debug(f"Found {len(results)} code results for query: {query}")
            return results

        except Exception as e:
            logger.warning(f"Code search failed: {e}")
            return []

    def get_rate_limit_info(self) -> Dict:
        """Get current rate limit information"""
        rate_limit = self.gh.get_rate_limit()
        return {
            "core": {
                "remaining": rate_limit.core.remaining,
                "limit": rate_limit.core.limit,
                "reset": rate_limit.core.reset
            },
            "search": {
                "remaining": rate_limit.search.remaining,
                "limit": rate_limit.search.limit,
                "reset": rate_limit.search.reset
            }
        }
