"""
islamweb.net adapter - Fatwa/Q&A scraping.
"""

from typing import List, Dict, Any, Optional
from scrapers.base import BaseScraper
from utils.text_cleaner import clean_text


class IslamWebAdapter(BaseScraper):
    """
    Adapter for islamweb.net fatwa/Q&A content.
    """
    
    def __init__(self, start_urls: Optional[List[str]] = None):
        """
        Initialize islamweb.net adapter.
        
        Args:
            start_urls: List of starting URLs
        """
        super().__init__(
            source_name='islamweb',
            base_url='https://www.islamweb.net'
        )
        self.start_urls_list = start_urls or [
            "https://www.islamweb.net/en/fatwa",
            "https://www.islamweb.net/ar/fatwa"
        ]
    
    def get_start_urls(self) -> List[str]:
        """Get starting URLs."""
        return self.start_urls_list
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse islamweb.net fatwa page.
        TODO: Implement site-specific parsing logic.
        """
        url = response.url
        
        # Basic extraction - to be customized per site structure
        title = response.css('h1::text').get() or response.css('.fatwa-title::text').get() or "Fatwa"
        content = response.css('.fatwa-content::text').getall() or response.css('article::text').getall()
        content_text = ' '.join(content) if content else ""
        
        if not content_text:
            return None
        
        return {
            'id': f"islamweb_{hash(url)}",
            'url': url,
            'title': clean_text(title),
            'content': clean_text(content_text),
            'content_type': 'q&a',
            'metadata': {
                'source_url': url
            },
            'language': self.detect_language(content_text)
        }

