"""
ahadith.co.uk adapter - Hadith collection scraping.
"""

from typing import List, Dict, Any, Optional
from scrapers.base import BaseScraper
from utils.text_cleaner import clean_text


class AhadithAdapter(BaseScraper):
    """
    Adapter for ahadith.co.uk hadith collections.
    """
    
    def __init__(self, start_urls: Optional[List[str]] = None):
        """
        Initialize ahadith.co.uk adapter.
        
        Args:
            start_urls: List of starting URLs
        """
        super().__init__(
            source_name='ahadith',
            base_url='https://ahadith.co.uk'
        )
        self.start_urls_list = start_urls or ["https://ahadith.co.uk"]
    
    def get_start_urls(self) -> List[str]:
        """Get starting URLs."""
        return self.start_urls_list
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse ahadith.co.uk hadith page.
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
            'id': f"ahadith_{hash(url)}",
            'url': url,
            'title': clean_text(title),
            'content': clean_text(content_text),
            'content_type': 'hadith',
            'metadata': {
                'source_url': url
            },
            'language': self.detect_language(content_text)
        }

