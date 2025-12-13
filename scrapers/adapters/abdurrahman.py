"""
abdurrahman.org adapter - Islamic articles and content scraping.
"""

from typing import List, Dict, Any, Optional
from scrapers.base import BaseScraper
from utils.text_cleaner import clean_text


class AbdurrahmanAdapter(BaseScraper):
    """
    Adapter for abdurrahman.org Islamic content.
    """
    
    def __init__(self, start_urls: Optional[List[str]] = None):
        """
        Initialize abdurrahman.org adapter.
        
        Args:
            start_urls: List of starting URLs
        """
        super().__init__(
            source_name='abdurrahman',
            base_url='https://abdurrahman.org'
        )
        self.start_urls_list = start_urls or ["https://abdurrahman.org"]
    
    def get_start_urls(self) -> List[str]:
        """Get starting URLs."""
        return self.start_urls_list
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse abdurrahman.org page.
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
            'id': f"abdurrahman_{hash(url)}",
            'url': url,
            'title': clean_text(title),
            'content': clean_text(content_text),
            'content_type': 'article',
            'metadata': {
                'source_url': url
            },
            'language': self.detect_language(content_text)
        }

