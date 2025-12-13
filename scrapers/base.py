"""
Base scraper interface for site-specific adapters.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Abstract base class for site-specific scrapers.
    All site adapters must inherit from this class and implement required methods.
    """
    
    def __init__(self, source_name: str, base_url: str):
        """
        Initialize base scraper.
        
        Args:
            source_name: Unique identifier for this source (e.g., 'islamqa', 'sunnah')
            base_url: Base URL of the site
        """
        self.source_name = source_name
        self.base_url = base_url
        self.logger = logging.getLogger(f"scraper.{source_name}")
    
    @abstractmethod
    def get_start_urls(self) -> List[str]:
        """
        Generate list of starting URLs to scrape.
        
        Returns:
            List of URL strings
        """
        pass
    
    @abstractmethod
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse a response and extract content.
        
        Args:
            response: Response object (Scrapy Response or similar)
            
        Returns:
            Dictionary with extracted content, or None if parsing failed
            Required keys: 'url', 'title', 'content', 'content_type'
            Optional keys: 'metadata' (dict), 'language'
        """
        pass
    
    def extract_content(self, response) -> Optional[Dict[str, Any]]:
        """
        Wrapper around parse() that handles common extraction logic.
        Can be overridden by subclasses for custom logic.
        
        Args:
            response: Response object
            
        Returns:
            Extracted content dictionary or None
        """
        return self.parse(response)
    
    def validate_content(self, content: Dict[str, Any]) -> bool:
        """
        Validate extracted content before saving.
        
        Args:
            content: Extracted content dictionary
            
        Returns:
            True if content is valid, False otherwise
        """
        required_keys = ['url', 'title', 'content', 'content_type']
        
        # Check required keys
        for key in required_keys:
            if key not in content:
                self.logger.warning(f"Missing required key '{key}' in content")
                return False
        
        # Validate content_type
        valid_types = ['q&a', 'hadith', 'article', 'metadata']
        if content['content_type'] not in valid_types:
            self.logger.warning(f"Invalid content_type: {content['content_type']}")
            return False
        
        # Check content is not empty
        if not content.get('title') or not content.get('content'):
            self.logger.warning("Empty title or content")
            return False
        
        # Validate URL
        if not content['url'] or not content['url'].startswith(('http://', 'https://')):
            self.logger.warning(f"Invalid URL: {content['url']}")
            return False
        
        return True
    
    def extract_metadata(self, response) -> Dict[str, Any]:
        """
        Extract site-specific metadata.
        Override in subclasses to extract custom metadata fields.
        
        Args:
            response: Response object
            
        Returns:
            Dictionary of metadata
        """
        return {}
    
    def detect_language(self, text: str) -> str:
        """
        Detect language of text (arabic/english/mixed).
        
        Args:
            text: Text to analyze
            
        Returns:
            Language identifier ('arabic', 'english', or 'mixed')
        """
        import re
        # Check for Arabic characters
        has_arabic = bool(re.search(r'[\u0600-\u06FF]', text))
        # Check for English (basic heuristic)
        has_english = bool(re.search(r'[a-zA-Z]', text))
        
        if has_arabic and has_english:
            return 'mixed'
        elif has_arabic:
            return 'arabic'
        else:
            return 'english'
    
    def normalize_url(self, url: str) -> str:
        """
        Normalize URL (remove fragments, sort query params, etc.).
        
        Args:
            url: URL string
            
        Returns:
            Normalized URL string
        """
        from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
        
        parsed = urlparse(url)
        # Remove fragment
        parsed = parsed._replace(fragment='')
        # Sort query parameters
        if parsed.query:
            params = parse_qs(parsed.query, keep_blank_values=True)
            sorted_params = sorted(params.items())
            parsed = parsed._replace(query=urlencode(sorted_params, doseq=True))
        
        return urlunparse(parsed)

