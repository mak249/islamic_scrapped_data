"""
shamela.ws adapter - Islamic books and articles scraping.
"""

from typing import List, Dict, Any, Optional
from scrapers.base import BaseScraper
from utils.text_cleaner import clean_text


class ShamelaAdapter(BaseScraper):
    """
    Adapter for shamela.ws Islamic library.
    """
    
    def __init__(self, start_urls: Optional[List[str]] = None):
        """
        Initialize shamela.ws adapter.
        
        Args:
            start_urls: List of starting URLs
        """
        super().__init__(
            source_name='shamela',
            base_url='https://shamela.ws'
        )
        self.start_urls_list = start_urls or ["https://shamela.ws"]
    
    def get_start_urls(self) -> List[str]:
        """Get starting URLs."""
        return self.start_urls_list
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse shamela.ws page.
        TODO: Implement site-specific parsing logic.
        """
        url = response.url
        
        # Basic extraction - to be customized per site structure
        title = response.css('h1::text').get() or response.css('title::text').get() or "Article"
        content = response.css('article::text').getall() or response.css('.content::text').getall()
        content_text = ' '.join(content) if content else ""
        
        if not content_text:
            return None
        
        return {
            'id': f"shamela_{hash(url)}",
            'url': url,
            'title': clean_text(title),
            'content': clean_text(content_text),
            'content_type': 'article',
            'metadata': {
                'source_url': url
            },
            'language': self.detect_language(content_text)
        }

