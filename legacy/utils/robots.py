"""
robots.txt parser and compliance checker.
"""

import time
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


class RobotsTxtChecker:
    """
    Manages robots.txt fetching, parsing, and compliance checking.
    Caches robots.txt files for 24 hours.
    """
    
    def __init__(self):
        self._cache: Dict[str, tuple] = {}  # domain -> (parser, fetch_time)
        self._cache_duration = 24 * 60 * 60  # 24 hours in seconds
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def _get_robots_url(self, url: str) -> str:
        """Get robots.txt URL for a given URL."""
        domain = self._get_domain(url)
        return urljoin(domain, '/robots.txt')
    
    def _fetch_robots_txt(self, url: str) -> Optional[RobotFileParser]:
        """
        Fetch and parse robots.txt for a domain.
        
        Args:
            url: Any URL from the domain
            
        Returns:
            RobotFileParser instance or None if fetch fails
        """
        domain = self._get_domain(url)
        robots_url = self._get_robots_url(url)
        
        # Check cache
        if domain in self._cache:
            parser, fetch_time = self._cache[domain]
            if time.time() - fetch_time < self._cache_duration:
                return parser
        
        # Fetch robots.txt
        parser = RobotFileParser()
        parser.set_url(robots_url)
        
        try:
            parser.read()
            self._cache[domain] = (parser, time.time())
            logger.info(f"Fetched robots.txt for {domain}")
            return parser
        except Exception as e:
            logger.warning(f"Failed to fetch robots.txt for {domain}: {e}")
            # Cache a None result to avoid repeated failures
            self._cache[domain] = (None, time.time())
            return None
    
    def can_fetch(self, url: str, user_agent: str = '*') -> bool:
        """
        Check if a URL can be fetched according to robots.txt.
        
        Args:
            url: URL to check
            user_agent: User agent string (default: '*' for all)
            
        Returns:
            True if allowed, False if disallowed, True if robots.txt unavailable
        """
        parser = self._fetch_robots_txt(url)
        if parser is None:
            # If robots.txt unavailable, default to allowing (fail open)
            return True
        return parser.can_fetch(user_agent, url)
    
    def get_crawl_delay(self, url: str, user_agent: str = '*') -> Optional[float]:
        """
        Get crawl-delay for a domain from robots.txt.
        
        Args:
            url: URL from the domain
            user_agent: User agent string
            
        Returns:
            Crawl delay in seconds, or None if not specified
        """
        parser = self._fetch_robots_txt(url)
        if parser is None:
            return None
        
        # RobotFileParser doesn't directly expose crawl_delay, but we can check
        # For now, return None - rate limiter will use default delays
        # In future, could extend RobotFileParser to parse crawl-delay
        return None

