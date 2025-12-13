"""
Per-domain rate limiter for polite crawling.
"""

import time
from typing import Dict, Optional
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Manages rate limiting per domain to ensure polite crawling.
    Tracks last request time per domain and enforces delays.
    """
    
    def __init__(self, default_delay: float = 1.0):
        """
        Initialize rate limiter.
        
        Args:
            default_delay: Default delay in seconds between requests (default: 1.0)
        """
        self.default_delay = default_delay
        self._last_request: Dict[str, float] = {}  # domain -> timestamp
        self._domain_delays: Dict[str, float] = {}  # domain -> custom delay
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return parsed.netloc
    
    def set_delay(self, url: str, delay: float):
        """
        Set custom delay for a specific domain.
        
        Args:
            url: URL from the domain
            delay: Delay in seconds
        """
        domain = self._get_domain(url)
        self._domain_delays[domain] = delay
        logger.debug(f"Set delay for {domain}: {delay}s")
    
    def wait_if_needed(self, url: str):
        """
        Wait if necessary to respect rate limits for the domain.
        
        Args:
            url: URL being requested
        """
        domain = self._get_domain(url)
        delay = self._domain_delays.get(domain, self.default_delay)
        
        if domain in self._last_request:
            elapsed = time.time() - self._last_request[domain]
            if elapsed < delay:
                wait_time = delay - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s for {domain}")
                time.sleep(wait_time)
        
        self._last_request[domain] = time.time()
    
    def reset(self, url: Optional[str] = None):
        """
        Reset rate limit tracking for a domain or all domains.
        
        Args:
            url: URL to reset, or None to reset all
        """
        if url:
            domain = self._get_domain(url)
            if domain in self._last_request:
                del self._last_request[domain]
        else:
            self._last_request.clear()

