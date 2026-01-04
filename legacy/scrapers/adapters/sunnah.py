"""sunnah.com adapter - Hadith collection scraping."""

import re
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from scrapers.storage import UnifiedStorage
from utils.text_cleaner import clean_text


class SunnahAdapter(BaseScraper):
    """Adapter for sunnah.com hadith collections."""

    DEFAULT_START_URLS = [
        "https://sunnah.com/bukhari",
        "https://sunnah.com/muslim",
        "https://sunnah.com/abudawud",
        "https://sunnah.com/tirmidhi",
        "https://sunnah.com/nasai",
        "https://sunnah.com/ibnmajah",
    ]

    def __init__(self, start_urls: Optional[List[str]] = None,
                 storage: Optional[UnifiedStorage] = None):
        """Initialize sunnah.com adapter."""
        super().__init__(
            source_name='sunnah',
            base_url='https://sunnah.com'
        )
        self.start_urls_list = start_urls or self.DEFAULT_START_URLS
        self.storage = storage
        self._visited_cache: Set[str] = set()

        if self.storage:
            try:
                self._visited_cache = self.storage.get_visited_urls(self.source_name)
            except Exception:
                self._visited_cache = set()

    def get_start_urls(self) -> List[str]:
        """Return starting URLs for the crawler."""
        return self.start_urls_list

    # ------------------------------------------------------------------
    # Public API expected by CoreEngine
    # ------------------------------------------------------------------
    def extract_content(self, response) -> Dict[str, Any]:
        """Extract hadith content or discover new URLs from the response."""
        html = self._get_html(response)
        soup = BeautifulSoup(html, 'html.parser')

        # First, attempt to parse actual hadith entries
        hadith_containers = soup.select('div.hadithTextContainers')
        if hadith_containers:
            return {
                'items': self._parse_hadith_entries(response.url, soup, hadith_containers)
            }

        # Otherwise, look for book index pages (list of sub-collections)
        book_links = self._discover_book_urls(response.url, soup)
        if book_links:
            return {'new_requests': book_links}

        # Fallback: no content and nothing new to queue
        return {}

    def parse(self, response):
        """Maintain compatibility with BaseScraper expectations."""
        return self.extract_content(response)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _get_html(response) -> str:
        if hasattr(response, 'text') and response.text:
            return response.text
        return response.body.decode('utf-8', errors='ignore')

    def _parse_hadith_entries(self, page_url: str, soup: BeautifulSoup, containers) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []

        collection_slug = self._extract_collection_slug(page_url)
        book_metadata = self._extract_book_metadata(soup, collection_slug)

        for container in containers:
            english_block = container.select_one('.english_hadith_full')
            english_text = clean_text(english_block.get_text(' ', strip=True)) if english_block else ""

            narrator_block = english_block.select_one('.hadith_narrated') if english_block else None
            narrator_text = clean_text(narrator_block.get_text(' ', strip=True)) if narrator_block else ""

            arabic_block = container.select_one('.arabic_hadith_full')
            arabic_text = clean_text(arabic_block.get_text(' ', strip=True)) if arabic_block else ""

            reference_label = container.find_previous('div', class_='hadith_reference_sticky')
            reference_text = clean_text(reference_label.get_text(' ', strip=True)) if reference_label else ""

            reference_table = container.find_next('table', class_='hadith_reference')
            reference_data = self._parse_reference_table(reference_table)

            metadata: Dict[str, Any] = {}
            metadata.update(book_metadata)
            metadata.update(reference_data)
            metadata['reference_label'] = reference_text
            metadata['source_page'] = page_url
            metadata['english_text'] = english_text
            if arabic_text:
                metadata['arabic_text'] = arabic_text

            canonical_url = metadata.get('reference_url') or page_url
            if canonical_url:
                canonical_url = canonical_url.strip()

            normalized_url = self.normalize_url(canonical_url) if canonical_url else page_url

            # Skip entries we have already stored
            if self._visited_cache and normalized_url in self._visited_cache:
                continue
            if self.storage and normalized_url not in self._visited_cache and self.storage.is_url_visited(normalized_url):
                self._visited_cache.add(normalized_url)
                continue

            relative_slug = metadata.get('reference_slug')

            hadith_number = metadata.get('hadith_number')
            if hadith_number is None and relative_slug:
                hadith_number = self._extract_hadith_number_from_slug(relative_slug)
                if hadith_number is not None:
                    metadata['hadith_number'] = hadith_number

            metadata['reference_url'] = normalized_url

            item_id = metadata.get('reference_slug')
            if item_id:
                item_id = item_id.strip('/')
                item_id = item_id.replace(':', '_').replace('/', '_')
            else:
                item_id = container.get('id') or f"{collection_slug}_{len(items)+1}"

            title_parts = [part for part in [reference_text, narrator_text] if part]
            title = " – ".join(title_parts) if title_parts else (english_text[:120] if english_text else reference_text or "Hadith")

            items.append({
                'id': f"{self.source_name}_{item_id}",
                'url': normalized_url,
                'title': title,
                'content': self._compose_content(english_text, arabic_text),
                'content_type': 'hadith',
                'metadata': metadata,
                'language': self._determine_language(english_text, arabic_text)
            })

            if self.storage:
                self._visited_cache.add(normalized_url)

        return items

    def _discover_book_urls(self, page_url: str, soup: BeautifulSoup) -> List[str]:
        links: List[str] = []

        # Primary book listing
        for anchor in soup.select('div.book_title a[href]'):
            href = anchor.get('href')
            if not href:
                continue
            if href.startswith('#') or href.lower().startswith('javascript'):
                continue
            links.append(urljoin(page_url, href))

        # Some collections expose chapter links directly
        for anchor in soup.select('div.chapter a[href]'):
            href = anchor.get('href')
            if not href:
                continue
            if href.startswith('#') or href.lower().startswith('javascript'):
                continue
            links.append(urljoin(page_url, href))

        # Deduplicate while preserving order
        seen = set()
        unique_links: List[str] = []
        for link in links:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)

        return unique_links

    @staticmethod
    def _compose_content(english_text: str, arabic_text: str) -> str:
        parts = []
        if english_text:
            parts.append(f"English: {english_text}")
        if arabic_text:
            parts.append(f"Arabic: {arabic_text}")
        return "\n\n".join(parts) if parts else english_text

    @staticmethod
    def _determine_language(english_text: str, arabic_text: str) -> str:
        has_english = bool(english_text)
        has_arabic = bool(arabic_text)
        if has_english and has_arabic:
            return 'mixed'
        if has_arabic:
            return 'arabic'
        return 'english'

    @staticmethod
    def _extract_collection_slug(url: str) -> str:
        parsed = urlparse(url)
        segments = [segment for segment in parsed.path.split('/') if segment]
        if not segments:
            return 'sunnah'
        # Handle urls like /bukhari:1 by splitting at colon
        first_segment = segments[0]
        return first_segment.split(':')[0]

    def _extract_book_metadata(self, soup: BeautifulSoup, collection_slug: str) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {'collection': collection_slug}

        english_collection = soup.select_one('.colindextitle .english')
        if english_collection:
            metadata['collection_name'] = clean_text(english_collection.get_text(' ', strip=True))

        book_number = soup.select_one('.book_page_number')
        if book_number:
            number_text = clean_text(book_number.get_text(' ', strip=True))
            metadata['book_number'] = number_text
            if number_text.isdigit():
                metadata['book_number_int'] = int(number_text)

        english_name = soup.select_one('.book_page_english_name')
        if english_name:
            metadata['book_english_name'] = clean_text(english_name.get_text(' ', strip=True))

        arabic_name = soup.select_one('.book_page_arabic_name')
        if arabic_name:
            metadata['book_arabic_name'] = clean_text(arabic_name.get_text(' ', strip=True))

        return metadata

    def _parse_reference_table(self, table) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        if not table:
            return data

        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 2:
                continue
            label_raw = clean_text(cells[0].get_text(' ', strip=True)).strip(':')
            value_text = clean_text(cells[1].get_text(' ', strip=True))

            label_key = re.sub(r"[^a-z0-9_]+", '_', label_raw.lower()).strip('_')

            if label_key == 'reference':
                link = cells[1].find('a')
                if link and link.get('href'):
                    href = link.get('href')
                    data['reference_slug'] = href
                    data['reference_url'] = urljoin('https://sunnah.com', href)
                    value_text = clean_text(link.get_text(' ', strip=True)) or value_text
            data[label_key] = value_text

            if label_key in {'in_book_reference', 'english_reference', 'hadith'}:
                extracted = self._extract_hadith_number_from_text(value_text)
                if extracted is not None:
                    data.setdefault('hadith_number', extracted)

        return data

    @staticmethod
    def _extract_hadith_number_from_text(text: str) -> Optional[int]:
        match = re.search(r'(?:Hadith|حديث)\s*(\d+)', text, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _extract_hadith_number_from_slug(slug: str) -> Optional[int]:
        if not slug:
            return None
        # Slugs look like /bukhari:123
        tail = slug.split(':')[-1]
        tail = tail.strip('/')
        if tail.isdigit():
            return int(tail)
        return None


