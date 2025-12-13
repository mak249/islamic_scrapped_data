# Contributing to Universal Islamic Data Scraper

Thank you for your interest in contributing! This guide will help you understand the codebase structure and how to add new site adapters.

## Project Structure

```
web scraping/
├── scrapers/
│   ├── base.py              # BaseScraper abstract class
│   ├── core.py              # CoreEngine (orchestrates scraping)
│   ├── storage.py           # Unified database storage
│   └── adapters/            # Site-specific adapters
│       ├── islamqa.py
│       ├── sunnah.py
│       └── ...
├── utils/
│   ├── robots.py            # robots.txt compliance
│   ├── rate_limiter.py      # Rate limiting
│   ├── deduplication.py    # Content deduplication
│   └── text_cleaner.py      # Text cleaning utilities
├── export/
│   └── formats.py           # AI training format exporters
├── config/
│   ├── scraper.yaml         # Global scraper settings
│   └── sites.yaml           # Site-specific configurations
├── main.py                  # CLI entry point
└── migrate_existing.py      # Migration script
```

## Architecture Overview

The scraper follows a modular adapter pattern:

1. **BaseScraper**: Abstract interface that all site adapters must implement
2. **CoreEngine**: Coordinates scraping, handles deduplication, rate limiting, robots.txt
3. **UnifiedStorage**: Single database schema for all sources
4. **Site Adapters**: Implement site-specific parsing logic

## Adding a New Site Adapter

### Step 1: Create the Adapter File

Create a new file in `scrapers/adapters/` (e.g., `scrapers/adapters/newsite.py`):

```python
"""
newsite.com adapter - Description of what this site contains.
"""

from typing import List, Dict, Any, Optional
from scrapers.base import BaseScraper
from utils.text_cleaner import clean_text


class NewsiteAdapter(BaseScraper):
    """
    Adapter for newsite.com.
    """
    
    def __init__(self, start_urls: Optional[List[str]] = None):
        super().__init__(
            source_name='newsite',
            base_url='https://newsite.com'
        )
        self.start_urls_list = start_urls or ["https://newsite.com"]
    
    def get_start_urls(self) -> List[str]:
        """Return list of starting URLs to scrape."""
        return self.start_urls_list
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse response and extract content.
        
        Returns:
            Dictionary with keys:
                - id: Unique identifier
                - url: Content URL
                - title: Content title
                - content: Content body
                - content_type: One of 'q&a', 'hadith', 'article', 'metadata'
                - metadata: Optional dict with site-specific fields
                - language: Optional language code
        """
        url = response.url
        
        # Extract content using CSS selectors or XPath
        title = response.css('h1::text').get() or "Title"
        content = response.css('.content::text').getall()
        content_text = ' '.join(content) if content else ""
        
        if not content_text:
            return None
        
        return {
            'id': f"newsite_{hash(url)}",
            'url': url,
            'title': clean_text(title),
            'content': clean_text(content_text),
            'content_type': 'article',  # or 'q&a', 'hadith', 'metadata'
            'metadata': {
                'source_url': url,
                # Add site-specific metadata here
            },
            'language': self.detect_language(content_text)
        }
```

### Step 2: Add Site Configuration

Add your site to `config/sites.yaml`:

```yaml
sites:
  newsite:
    enabled: true
    base_url: "https://newsite.com"
    start_urls:
      - "https://newsite.com/page1"
      - "https://newsite.com/page2"
    download_delay: 2.0
    concurrent_requests: 4
    content_type: "article"  # or "q&a", "hadith", "metadata"
```

### Step 3: Register Adapter in CLI (Optional)

To make your adapter accessible via CLI, update `main.py`:

```python
from scrapers.adapters.newsite import NewsiteAdapter

# In cmd_scrape function:
elif args.site == 'newsite':
    start_urls = site_config.get('start_urls', [])
    adapter = NewsiteAdapter(start_urls=start_urls)
```

### Step 4: Test Your Adapter

Test your adapter:

```python
from scrapers.adapters.newsite import NewsiteAdapter
from scrapers.core import CoreEngine
from scrapers.storage import UnifiedStorage

# Initialize
storage = UnifiedStorage("test.db")
adapter = NewsiteAdapter()
engine = CoreEngine(db_path="test.db")

# Test parsing (requires a Scrapy response object)
# Or run via CLI:
# python main.py scrape --site newsite
```

## Code Conventions

### Naming

- **Adapters**: `{SiteName}Adapter` (e.g., `IslamQAAdapter`)
- **Files**: lowercase with underscores (e.g., `islam_qa.py`)
- **Source names**: lowercase, no spaces (e.g., `islamqa`, `sunnah`)

### Content Types

Use one of these content types:
- `q&a`: Question and answer pairs
- `hadith`: Hadith collections
- `article`: Articles and essays
- `metadata`: Metadata only (title, URL, date, author, description)

### Required Methods

All adapters must implement:

1. `get_start_urls()`: Return list of starting URLs
2. `parse(response)`: Extract content from response
3. Optionally override `validate_content()` for custom validation

### Content Format

The `parse()` method should return a dictionary with:

```python
{
    'id': 'unique_id',           # Required
    'url': 'https://...',        # Required
    'title': 'Title',            # Required
    'content': 'Full content',  # Required
    'content_type': 'q&a',      # Required: 'q&a', 'hadith', 'article', 'metadata'
    'metadata': {...},           # Optional: site-specific fields
    'language': 'english'       # Optional: auto-detected if not provided
}
```

### Metadata Fields

Include relevant metadata:
- Author name
- Publication date
- Categories/tags
- References/citations
- Source book/collection (for hadith)
- Fatwa number (for Q&A)

## Best Practices

### 1. Respect robots.txt

The CoreEngine automatically checks robots.txt. If your site has specific requirements, document them.

### 2. Rate Limiting

Use appropriate delays:
- Default: 1-2 seconds between requests
- Adjust per site in `config/sites.yaml`
- Be respectful of server resources

### 3. Error Handling

Handle common errors gracefully:
- 404 (page not found)
- 403 (forbidden)
- Timeouts
- Malformed HTML

### 4. Text Cleaning

Always use `clean_text()` from `utils.text_cleaner`:
- Removes HTML tags
- Decodes HTML entities
- Normalizes whitespace

### 5. Deduplication

The CoreEngine handles deduplication automatically using content hashes. You don't need to implement this yourself.

### 6. Resume Support

For ID-based sites (like islamqa.info), implement resume support:
- Store last scraped ID in resume state
- Check resume state in `__init__`
- Update resume state after scraping

Example:

```python
def __init__(self, start_id=1, end_id=10000, storage=None):
    super().__init__(source_name='newsite', base_url='https://newsite.com')
    self.start_id = start_id
    self.end_id = end_id
    
    # Check resume state
    if storage:
        resume_state = storage.get_resume_state('newsite')
        if resume_state and resume_state.get('last_id'):
            self.start_id = max(start_id, resume_state['last_id'] + 1)
```

## Testing Guidelines

### Unit Tests

Test your adapter's parsing logic:

```python
import unittest
from scrapy.http import HtmlResponse
from scrapers.adapters.newsite import NewsiteAdapter

class TestNewsiteAdapter(unittest.TestCase):
    def test_parse(self):
        adapter = NewsiteAdapter()
        html = "<html><body><h1>Test</h1><div class='content'>Content here</div></body></html>"
        response = HtmlResponse(url='https://newsite.com/test', body=html.encode())
        result = adapter.parse(response)
        self.assertIsNotNone(result)
        self.assertEqual(result['title'], 'Test')
```

### Integration Tests

Test full scraping workflow:

```python
from scrapers.core import CoreEngine
from scrapers.adapters.newsite import NewsiteAdapter
from scrapers.storage import UnifiedStorage

def test_scraping():
    storage = UnifiedStorage("test.db")
    adapter = NewsiteAdapter()
    engine = CoreEngine(db_path="test.db")
    
    # Scrape a small sample
    engine.scrape_site(adapter, concurrent_requests=1, download_delay=1.0)
    
    # Verify data was saved
    stats = storage.get_stats()
    assert stats['total'] > 0
```

## Documentation

### Docstrings

Include docstrings for all classes and methods:

```python
def parse(self, response) -> Optional[Dict[str, Any]]:
    """
    Parse newsite.com page and extract content.
    
    Args:
        response: Scrapy Response object
        
    Returns:
        Dictionary with extracted content or None if parsing failed
    """
```

### Comments

Add comments for:
- Complex parsing logic
- Site-specific quirks
- Non-obvious selectors
- Workarounds for site issues

## Ethical Considerations

### Copyright

- **Metadata-only**: For sites with copyright restrictions, only scrape metadata (title, URL, date, author, description)
- **Public domain**: Full content scraping is acceptable for public domain sources
- **Attribution**: Always include source URL and attribution in metadata

### Rate Limiting

- Always respect robots.txt
- Use appropriate delays between requests
- Don't overwhelm servers
- Monitor for rate limit errors (429) and adjust accordingly

### Terms of Service

- Review site's Terms of Service before scraping
- Some sites may prohibit scraping - respect these restrictions
- When in doubt, scrape metadata only or contact site owner

## Submitting Changes

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/newsite-adapter`
3. **Make your changes**: Add adapter, update configs, add tests
4. **Test thoroughly**: Run tests and verify scraping works
5. **Update documentation**: Add your site to README if applicable
6. **Submit pull request**: Include description of what was added

## Questions?

If you have questions or need help:
1. Check existing adapters for examples
2. Review the BaseScraper interface in `scrapers/base.py`
3. Open an issue with your question

Thank you for contributing!

