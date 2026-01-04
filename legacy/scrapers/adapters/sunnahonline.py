"""
sunnahonline.com adapter - Hadith collection scraping.
"""

from typing import List, Dict, Any, Optional
from scrapers.base import BaseScraper
from utils.text_cleaner import clean_text


class SunnahOnlineAdapter(BaseScraper):
    """
    Adapter for sunnahonline.com hadith collections.
    """
    
    def __init__(self, start_urls: Optional[List[str]] = None):
        """
        Initialize sunnahonline.com adapter.
        
        Args:
            start_urls: List of starting URLs
        """
        super().__init__(
            source_name='sunnahonline',
            base_url='https://sunnahonline.com'
        )
        self.start_urls_list = start_urls or ["https://sunnahonline.com"]
    
    def get_start_urls(self) -> List[str]:
        """Get starting URLs."""
        return self.start_urls_list
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse sunnahonline.com hadith page.
        TODO: Implement site-specific parsing logic.
        """
        url = response.url
        
        # Basic extraction - to be customized per site structure
        title = response.css('h1::text').get() or response.css('title::text').get() or "Hadith"
        content = response.css('article::text').getall() or response.css('.hadith-text::text').getall()
        content_text = ' '.join(content) if content else ""
        
        if not content_text:
            return None
        
        return {
            'id': f"sunnahonline_{hash(url)}",
            'url': url,
            'title': clean_text(title),
            'content': clean_text(content_text),
            'content_type': 'hadith',
            'metadata': {
                'source_url': url
            },
            'language': self.detect_language(content_text)
        }

