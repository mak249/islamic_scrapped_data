"""
islamqa.info adapter - Q&A scraping with ID-based URL generation.
Migrated from fast_scraper.py preserving existing functionality.
Uses Playwright as fallback for JavaScript-rendered pages.
"""

import re
import time
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from scrapers.storage import UnifiedStorage
from utils.text_cleaner import clean_text, contains_html

# Try to import Playwright for JavaScript rendering fallback
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class IslamQAAdapter(BaseScraper):
    """
    Adapter for islamqa.info Q&A scraping.
    Uses ID-based URL generation: https://islamqa.info/en/answers/{id}
    Supports resume by tracking last scraped ID.
    """
    
    def __init__(self, start_id: int = 1, end_id: int = 10000, 
                 storage: Optional[UnifiedStorage] = None):
        """
        Initialize islamqa.info adapter.
        
        Args:
            start_id: Starting ID for URL generation
            end_id: Ending ID for URL generation
            storage: Optional storage instance for resume support
        """
        super().__init__(
            source_name='islamqa',
            base_url='https://islamqa.info'
        )
        self.start_id = start_id
        self.end_id = end_id
        self.storage = storage
        
        # Check resume state if storage provided
        if storage:
            resume_state = storage.get_resume_state('islamqa')
            if resume_state and resume_state.get('last_id'):
                self.start_id = max(start_id, resume_state['last_id'] + 1)
                self.logger.info(f"Resuming from ID {self.start_id}")
    
    def get_start_urls(self) -> List[str]:
        """
        Generate list of URLs based on ID range.
        
        Returns:
            List of URLs to scrape
        """
        urls = []
        for i in range(self.start_id, self.end_id + 1):
            # Support both English and Arabic
            urls.append(f"https://islamqa.info/en/answers/{i}")
            # Optionally add Arabic URLs
            # urls.append(f"https://islamqa.info/ar/answers/{i}")
        return urls
    
    def parse(self, response) -> Optional[Dict[str, Any]]:
        """
        Parse islamqa.info Q&A page.
        
        Args:
            response: Scrapy Response object
            
        Returns:
            Dictionary with extracted Q&A data or None
        """
        url = response.url
        
        # Skip non-200 responses (404s are gaps in ID sequence)
        if getattr(response, 'status', 200) != 200:
            return None
        
        # islamqa.info is a Next.js site - ALL content is JavaScript-rendered
        # Use Playwright directly instead of trying static HTML extraction first
        if PLAYWRIGHT_AVAILABLE:
            result = self._extract_with_playwright(url)
            return result
        else:
            # Fallback to static HTML if Playwright not available (won't work for Next.js)
            print(f"⚠️  Playwright not available - static HTML extraction will likely fail for Next.js site")
            
            # Check for language-only redirect pages (shows "This page is available in the following languages")
            try:
                html_content = response.text if hasattr(response, 'text') else response.body.decode('utf-8', errors='ignore')
                # Check if HTML tag has error ID (more reliable than text search)
                if '<html id="__next_error__"' in html_content or '<html id=\'__next_error__\'' in html_content:
                    return None
                # Check for language-only redirect pages (shows "This page is available in the following languages")
                if 'This page is available in the following languages' in html_content:
                    self.logger.debug(f"Skipping {url} - language-only redirect page (no content in this language)")
                    return None
            except:
                pass
            
            # Extract question
            question = self._extract_question(response)
            if not question or question == "No question found":
                return None
            
            # Extract answer
            answer = self._extract_answer(response)
            if not answer or answer == "No answer found":
                return None
        
        # Final validation - reject if contains HTML
        if contains_html(question) or contains_html(answer):
            self.logger.warning(f"Skipping {url} - contains HTML in extracted content")
            return None
        
        # Extract fatwa number
        fatwa_number = self._extract_fatwa_number(f"{question} {answer}")
        
        # Detect language
        language = self.detect_language(f"{question} {answer}")
        
        # Extract ID from URL
        url_id = self._extract_id_from_url(url)
        
        # Combine question and answer for content field
        content = f"Question: {question}\n\nAnswer: {answer}"
        
        return {
            'id': f"islamqa_{url_id}_{int(time.time())}",
            'url': url,
            'title': question,
            'content': content,
            'content_type': 'q&a',
            'metadata': {
                'fatwa_number': fatwa_number,
                'question_id': url_id,
                'question': question,
                'answer': answer,
                'word_count': len(answer.split()),
                'quality_score': min(1.0, len(answer.split()) / 100.0)
            },
            'language': language
        }
    
    def _extract_question(self, response) -> str:
        """Extract question text from response."""
        # Parse full HTML with BeautifulSoup for more reliable extraction
        # Use response.body if response.text fails
        try:
            html_content = response.text
        except:
            html_content = response.body.decode('utf-8', errors='ignore')
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Method 1: Try h1 with SUT_question_title class (title of the page)
        h1_title = soup.find('h1', class_=lambda c: c and 'SUT_question_title' in str(c))
        if h1_title:
            question = clean_text(h1_title.get_text())
            if question and len(question) > 10 and "Islam Question" not in question:
                return question
        
        # Method 2: Find question section and extract from div after Question h2
        question_section = soup.find('section', class_=lambda c: c and 'question' in str(c).lower())
        if question_section:
            # Find h2 with "Question" text
            h2 = question_section.find('h2', string=lambda t: t and 'Question' in str(t))
            if h2:
                # Get the div sibling after h2
                next_div = h2.find_next_sibling('div')
                if next_div:
                    question = clean_text(next_div.get_text())
                    if question and len(question) > 10:
                        return question
            # Alternative: find div with text-gray-900 class in question section
            question_div = question_section.find('div', class_=lambda c: c and 'text-gray-900' in str(c))
            if question_div:
                question = clean_text(question_div.get_text())
                if question and len(question) > 10:
                    return question
        
        # Method 3: Try CSS selectors as fallback
        question_selectors = [
            'h1.SUT_question_title',
            'h1[class*="question"]',
            '.SUT_question_number + div',
            'section[class*="question"] h2 + div',
            'h1.title',
            'h1',
        ]
        
        for selector in question_selectors:
            q_elem = response.css(selector).get()
            if q_elem:
                question = clean_text(q_elem)
                if question and len(question) > 10 and not contains_html(question):
                    if "Islam Question" not in question and "Question & Answer" not in question:
                        return question
        
        return "No question found"
    
    def _extract_answer(self, response) -> str:
        """Extract answer text from response."""
        # Parse full HTML with BeautifulSoup for more reliable extraction
        # Use response.body if response.text fails
        try:
            html_content = response.text
        except:
            html_content = response.body.decode('utf-8', errors='ignore')
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Method 1: Find div with SUT_answer_text class
        answer_div = soup.find('div', class_=lambda c: c and ('SUT_answer_text' in str(c) or 'post-body' in str(c).lower()))
        if answer_div:
            # Remove unwanted elements
            for tag in answer_div(["script", "style", "nav", "aside", "footer", "header", "dialog", "button", "form"]):
                tag.decompose()
            answer = answer_div.get_text(separator=' ', strip=True)
            answer = re.sub(r'\s+', ' ', answer).strip()
            if answer and len(answer) > 20:
                return answer
        
        # Method 2: Find section with Answer heading
        sections = soup.find_all('section')
        for section in sections:
            h2 = section.find('h2', string=lambda t: t and 'Answer' in str(t))
            if h2:
                # Find answer div in this section
                answer_div = section.find('div', class_=lambda c: c and ('post' in str(c).lower() or 'answer' in str(c).lower() or 'SUT' in str(c)))
                if not answer_div:
                    # Get all divs in section and find the one with most text
                    divs = section.find_all('div')
                    for div in divs:
                        text = div.get_text(strip=True)
                        if len(text) > 100:  # Answer should be substantial
                            # Remove unwanted elements
                            for tag in div(["script", "style", "nav", "aside", "footer", "header", "dialog", "button", "form"]):
                                tag.decompose()
                            answer = div.get_text(separator=' ', strip=True)
                            answer = re.sub(r'\s+', ' ', answer).strip()
                            if answer and len(answer) > 20:
                                return answer
                else:
                    # Remove unwanted elements
                    for tag in answer_div(["script", "style", "nav", "aside", "footer", "header", "dialog", "button", "form"]):
                        tag.decompose()
                    answer = answer_div.get_text(separator=' ', strip=True)
                    answer = re.sub(r'\s+', ' ', answer).strip()
                    if answer and len(answer) > 20:
                        return answer
        
        # Method 3: Try CSS selectors as fallback
        answer_selectors = [
            '.SUT_answer_text',
            '.post-body_postBody__TVZCQ',
            'article#single-post-content',
            'article.single-post-content',
            '.post-content',
            '.entry-content',
        ]
        
        for selector in answer_selectors:
            answer_elem = response.css(selector).get()
            if answer_elem:
                soup_elem = BeautifulSoup(answer_elem, 'html.parser')
                for tag in soup_elem(["script", "style", "nav", "aside", "footer", "header", "dialog"]):
                    tag.decompose()
                answer = soup_elem.get_text(separator=' ', strip=True)
                answer = re.sub(r'\s+', ' ', answer).strip()
                if answer and len(answer) > 20 and not contains_html(answer):
                    return answer
        
        return "No answer found"
    
    def _extract_fatwa_number(self, text: str) -> str:
        """Extract fatwa number from text."""
        fatwa_patterns = [
            r'Fatwa\s*No\.?\s*(\d+)',
            r'Question\s*No\.?\s*(\d+)',
            r'Answer\s*No\.?\s*(\d+)',
            r'ID:\s*(\d+)'
        ]
        
        for pattern in fatwa_patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                return m.group(1)
        return ""
    
    def _extract_id_from_url(self, url: str) -> int:
        """Extract numeric ID from islamqa.info URL."""
        match = re.search(r'/answers/(\d+)', url)
        if match:
            return int(match.group(1))
        return 0
    
    def _extract_with_playwright(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Fallback extraction using Playwright for JavaScript-rendered pages.
        
        Args:
            url: URL to scrape
            
        Returns:
            Dictionary with extracted Q&A data or None
        """
        if not PLAYWRIGHT_AVAILABLE:
            self.logger.warning("Playwright not available. Install with: pip install playwright && playwright install")
            return None
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Navigate to page
                page.goto(url, wait_until='networkidle', timeout=30000)
                
                # Wait for JavaScript to render content (Next.js needs time)
                page.wait_for_timeout(3000)
                
                # Check for 404 pages or language redirects first
                html_content = page.content()
                if '<html id="__next_error__"' in html_content or '404: This page could not be found' in html_content:
                    browser.close()
                    return None
                
                if 'This page is available in the following languages' in html_content:
                    browser.close()
                    return None
                
                # Wait for actual content selectors (more flexible)
                try:
                    # Wait for any content indicators
                    page.wait_for_selector('h1, section, div[class*="answer"], div[class*="post"]', timeout=10000)
                except PlaywrightTimeoutError:
                    # Maybe it's still loading, wait a bit more
                    page.wait_for_timeout(2000)
                    html_content = page.content()
                    # Final check - if still no content indicators, skip
                    if 'SUT_question_title' not in html_content and 'SUT_answer_text' not in html_content and 'question' not in html_content.lower():
                        browser.close()
                        return None
                
                # Get final rendered HTML
                html_content = page.content()
                browser.close()
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Extract question
                h1_title = soup.find('h1', class_=lambda c: c and 'SUT_question_title' in str(c))
                if not h1_title:
                    # Try alternative methods
                    question_section = soup.find('section', class_=lambda c: c and 'question' in str(c).lower())
                    if question_section:
                        question_div = question_section.find('div', class_=lambda c: c and 'text-gray-900' in str(c))
                        if question_div:
                            question = clean_text(question_div.get_text())
                        else:
                            question = None
                    else:
                        question = None
                else:
                    question = clean_text(h1_title.get_text())
                
                if not question or len(question) < 10:
                    return None
                
                # Extract answer
                answer_div = soup.find('div', class_=lambda c: c and ('SUT_answer_text' in str(c) or 'post-body' in str(c).lower()))
                if not answer_div:
                    # Try finding in sections
                    sections = soup.find_all('section')
                    for section in sections:
                        h2 = section.find('h2', string=lambda t: t and 'Answer' in str(t))
                        if h2:
                            answer_div = section.find('div', class_=lambda c: c and ('post' in str(c).lower() or 'answer' in str(c).lower()))
                            if answer_div:
                                break
                
                if answer_div:
                    # Remove unwanted elements
                    for tag in answer_div(["script", "style", "nav", "aside", "footer", "header", "dialog", "button", "form"]):
                        tag.decompose()
                    answer = answer_div.get_text(separator=' ', strip=True)
                    answer = re.sub(r'\s+', ' ', answer).strip()
                else:
                    answer = None
                
                if not answer or len(answer) < 20:
                    return None
                
                # Final validation
                if contains_html(question) or contains_html(answer):
                    self.logger.warning(f"Skipping {url} - contains HTML in extracted content")
                    return None
                
                # Extract metadata
                fatwa_number = self._extract_fatwa_number(f"{question} {answer}")
                language = self.detect_language(f"{question} {answer}")
                url_id = self._extract_id_from_url(url)
                
                # Combine question and answer for content field
                content = f"Question: {question}\n\nAnswer: {answer}"
                
                return {
                    'id': f"islamqa_{url_id}_{int(time.time())}",
                    'url': url,
                    'title': question,
                    'content': content,
                    'content_type': 'q&a',
                    'metadata': {
                        'fatwa_number': fatwa_number,
                        'question_id': url_id,
                        'question': question,
                        'answer': answer,
                        'word_count': len(answer.split()),
                        'quality_score': min(1.0, len(answer.split()) / 100.0),
                        'extracted_with': 'playwright'
                    },
                    'language': language
                }
        
        except Exception as e:
            self.logger.error(f"Playwright extraction failed for {url}: {e}", exc_info=True)
            return None
    
    @staticmethod
    def get_last_scraped_id(storage: UnifiedStorage) -> int:
        """
        Get the highest numeric ID scraped from islamqa.info.
        
        Args:
            storage: UnifiedStorage instance
            
        Returns:
            Highest ID or 0 if none found
        """
        resume_state = storage.get_resume_state('islamqa')
        if resume_state and resume_state.get('last_id'):
            return resume_state['last_id']
        
        # Try to extract from URLs
        contents = storage.query_content(source='islamqa', limit=1000)
        max_id = 0
        for content in contents:
            url = content.get('url', '')
            match = re.search(r'/answers/(\d+)', url)
            if match:
                max_id = max(max_id, int(match.group(1)))
        return max_id
    
    @staticmethod
    def estimate_remaining(storage: UnifiedStorage, max_id: int) -> int:
        """
        Estimate remaining IDs to scrape.
        
        Args:
            storage: UnifiedStorage instance
            max_id: Maximum ID to scrape
            
        Returns:
            Estimated remaining count
        """
        last_id = IslamQAAdapter.get_last_scraped_id(storage)
        return max(0, max_id - last_id)

