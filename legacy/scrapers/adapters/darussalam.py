"""
darussalam.com adapter - Metadata-only scraping (title, URL, date, author, description).
"""

from typing import List, Dict, Any, Optional
from scrapers.base import BaseScraper
from utils.text_cleaner import clean_text


class DarussalamAdapter(BaseScraper):
    """
    Adapter for darussalam.com - metadata only (respects copyright).
    """
    
    def __init__(self, start_urls: Optional[List[str]] = None):
        """
        Initialize darussalam.com adapter.
        
        Args:
            start_urls: List of starting URLs
        """
        super().__init__(
            source_name='darussalam',
            base_url='https://darussalam.com'
        )
        self.start_urls_list = start_urls or ["https://darussalam.com"]
    
    def get_start_urls(self) -> List[str]:
        """Get starting URLs."""
        return self.start_urls_list
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse darussalam.com page - extract metadata only.
        TODO: Implement site-specific parsing logic.
        """
        url = response.url
        
        # Extract metadata fields
        title = response.css('h1::text').get() or response.css('title::text').get() or "Publication"
        
        # Try to extract author
        author = response.css('.author::text').get() or response.css('[itemprop="author"]::text').get() or ""
        
        # Try to extract date
        date = response.css('.date::text').get() or response.css('[itemprop="datePublished"]::text').get() or ""
        
        # Extract description/excerpt (first ~200 chars)
        description_elem = response.css('.description::text').get() or response.css('.excerpt::text').get() or response.css('meta[name="description"]::attr(content)').get() or ""
        description = clean_text(description_elem)[:200] if description_elem else ""
        
        # For metadata-only, content is just the description
        content = description or title
        
        return {
            'id': f"darussalam_{hash(url)}",
            'url': url,
            'title': clean_text(title),
            'content': content,  # Limited to description/excerpt
            'content_type': 'metadata',
            'metadata': {
                'author': clean_text(author),
                'date': clean_text(date),
                'description': description,
                'source_url': url,
                'note': 'Metadata only - full content not scraped due to copyright'
            },
            'language': self.detect_language(content)
        }

