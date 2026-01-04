"""
Core engine that coordinates scraping, deduplication, rate limiting, and robots.txt compliance.
"""

from typing import List, Optional
import logging
from datetime import datetime
import time

from scrapers.base import BaseScraper
from scrapers.storage import UnifiedStorage
from utils.robots import RobotsTxtChecker
from utils.rate_limiter import RateLimiter
from utils.deduplication import compute_content_hash
from utils.text_cleaner import clean_text, contains_html

logger = logging.getLogger(__name__)


# Note: Scrapy is imported lazily inside scrape_site to allow non-scraping
# commands (like status/export) to run without Scrapy installed.


class CoreEngine:
    """
    Core engine that coordinates scraping across multiple sites.
    """
    
    def __init__(self, db_path: str = "islamic_data.db", 
                 default_delay: float = 1.0,
                 user_agent: str = None):
        """
        Initialize core engine.
        
        Args:
            db_path: Path to database file
            default_delay: Default delay between requests (seconds)
            user_agent: User agent string (default: polite scraper)
        """
        self.storage = UnifiedStorage(db_path)
        self.robots_checker = RobotsTxtChecker()
        self.rate_limiter = RateLimiter(default_delay=default_delay)
        self.user_agent = user_agent or 'IslamicDataScraper/1.0 (+https://github.com/your-repo)'
        
        logger.info("CoreEngine initialized")
    
    def scrape_site(self, adapter: BaseScraper, 
                   concurrent_requests: int = 8,
                   download_delay: float = None,
                   log_level: str = 'INFO',
                   disable_rate_limit: bool = False,
                   simple_output: bool = False,
                   fast_mode: bool = False,
                   **scrapy_settings):
        """
        Scrape a single site using its adapter.
        
        Args:
            adapter: BaseScraper instance
            concurrent_requests: Number of concurrent requests
            download_delay: Delay between requests (overrides default)
            **scrapy_settings: Additional Scrapy settings
        """
        logger.info(f"Starting scrape for {adapter.source_name}")
        print(f"\n{'='*80}")
        print(f">> STARTING SCRAPE: {adapter.source_name}")
        print(f"{'='*80}\n")

        # Suppress noisy loggers based on log_level and simple_output
        if simple_output:
            # In simple mode, silence ALL Scrapy logging
            target_level = logging.ERROR
        else:
            target_level = getattr(logging, log_level.upper(), logging.INFO)
        
        logging.getLogger('utils.rate_limiter').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        logging.getLogger('scrapy').setLevel(target_level)
        logging.getLogger('scrapy.utils.log').setLevel(target_level)
        logging.getLogger('scrapy.middleware').setLevel(target_level)
        logging.getLogger('scrapy.crawler').setLevel(target_level)
        logging.getLogger('scrapy.core.engine').setLevel(target_level)
        logging.getLogger('scrapy.core.scraper').setLevel(target_level)
        logging.getLogger('scrapy.extensions').setLevel(target_level)
        logging.getLogger('scrapy.statscollectors').setLevel(target_level)
        logging.getLogger('scrapy.downloadermiddlewares').setLevel(target_level)
        logging.getLogger('scrapy.downloadermiddlewares.retry').setLevel(target_level)
        logging.getLogger('scrapy.core.downloader.handlers.http11').setLevel(logging.ERROR)
        logging.getLogger('py.warnings').setLevel(logging.ERROR)

        # Lazy import Scrapy components so non-scraping commands don't require it
        try:
            import scrapy  # type: ignore
            from scrapy.crawler import CrawlerProcess  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "Scrapy is required to run scraping. Install with: pip install scrapy twisted-iocpsupport"
            ) from e
        
        # Set custom delay if provided
        if download_delay:
            self.rate_limiter.set_delay(adapter.base_url, download_delay)
        
        # Configure Scrapy settings
        actual_delay = 0.0 if fast_mode else (download_delay if download_delay is not None else 1.0)
        actual_concurrency = 24 if fast_mode else concurrent_requests
        actual_per_domain = 12 if fast_mode else max(1, concurrent_requests // 2)
        
        settings = {
            'USER_AGENT': self.user_agent,
            'ROBOTSTXT_OBEY': False,  # We handle robots.txt manually
            'DOWNLOAD_DELAY': actual_delay,
            'RANDOMIZE_DOWNLOAD_DELAY': 0.0 if fast_mode else 0.5,
            'CONCURRENT_REQUESTS': actual_concurrency,
            'CONCURRENT_REQUESTS_PER_DOMAIN': actual_per_domain,
            'DOWNLOAD_TIMEOUT': 30,
            'RETRY_TIMES': 2,
            'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
            'HTTPERROR_ALLOWED_CODES': [404, 403],
            'COOKIES_ENABLED': False,
            'DNSCACHE_ENABLED': True,
            'LOG_LEVEL': 'ERROR' if simple_output else log_level,  # Only show errors in simple mode
            **scrapy_settings
        }
        if simple_output:
            # Reduce Scrapy noise for human-friendly output
            settings.update({
                'TELNETCONSOLE_ENABLED': False,
                'EXTENSIONS': {
                    'scrapy.extensions.telnet.TelnetConsole': None,
                    'scrapy.extensions.logstats.LogStats': None,
                }
            })
        
        # Define the spider class lazily to avoid referencing scrapy at import time
        engine_self = self
        rate_limit_enabled = not disable_rate_limit
        progress_last = {'t': time.time()}
        simple_output_flag = simple_output

        class ScraperSpider(scrapy.Spider):  # type: ignore
            name = f"{adapter.source_name}_spider"

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.adapter = adapter
                self.storage = engine_self.storage
                self.robots_checker = engine_self.robots_checker
                self.rate_limiter = engine_self.rate_limiter
                self.scraped_count = 0
                self.skipped_count = 0
                self.error_count = 0
                self.start_urls = adapter.get_start_urls()
                logger.info(
                    f"Initialized spider for {adapter.source_name} with {len(self.start_urls)} start URLs"
                )
                
                # Re-apply log level suppression after Scrapy initializes its loggers
                if simple_output_flag:
                    for log_name in ['scrapy', 'scrapy.core.engine', 'scrapy.core.scraper',
                                    'scrapy.downloadermiddlewares', 'scrapy.downloadermiddlewares.retry']:
                        logging.getLogger(log_name).setLevel(logging.ERROR)

            def start_requests(self):
                total_urls = len(self.start_urls)
                already_visited = 0
                urls_to_scrape = []
                
                # Pre-load all visited URLs into a set for fast lookup
                print("Loading visited URLs from database...")
                visited_urls_set = self.storage.get_visited_urls(adapter.source_name)
                print(f"Loaded {len(visited_urls_set)} visited URLs")
                
                for url in self.start_urls:
                    if not self.robots_checker.can_fetch(
                        url, user_agent=self.settings.get('USER_AGENT', '*')
                    ):
                        logger.warning(f"robots.txt disallows: {url}")
                        self.skipped_count += 1
                        continue
                    
                    # Use the pre-loaded set for O(1) lookup instead of database query
                    if url in visited_urls_set:
                        already_visited += 1
                        continue  # Skip silently, we'll show summary
                    
                    # This URL will be scraped
                    urls_to_scrape.append(url)
                
                # Show summary of what will be scraped
                new_urls = len(urls_to_scrape)
                if already_visited > 0:
                    print(f"\n>> Summary: {already_visited} URLs already scraped, {new_urls} URLs will be processed\n")
                
                # Yield requests for URLs to scrape
                for url in urls_to_scrape:
                    if rate_limit_enabled:
                        # Suppress rate limiter logging - it's too noisy
                        old_level = logging.getLogger('utils.rate_limiter').level
                        logging.getLogger('utils.rate_limiter').setLevel(logging.WARNING)
                        self.rate_limiter.wait_if_needed(url)
                        logging.getLogger('utils.rate_limiter').setLevel(old_level)
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse,
                        errback=self.errback,
                        dont_filter=False,
                        meta={'adapter': self.adapter}
                    )

            def parse(self, response):
                from urllib.parse import urljoin

                adapter_local = response.meta.get('adapter', self.adapter)

                def _looks_like_content(data):
                    required_keys = {'url', 'title', 'content', 'content_type'}
                    return isinstance(data, dict) and required_keys.issubset(set(data.keys()))

                try:
                    # Show what we're trying to scrape
                    status = getattr(response, 'status', 'unknown')
                    if status == 200 and not simple_output_flag:
                        print(f"📄 Processing: {response.url}")

                    raw_result = adapter_local.extract_content(response)

                    items_to_save = []
                    new_requests = []

                    if isinstance(raw_result, list):
                        items_to_save = raw_result
                    elif isinstance(raw_result, dict):
                        potential_items = []
                        if 'items' in raw_result and isinstance(raw_result['items'], list):
                            potential_items.extend(raw_result['items'])
                        if 'item' in raw_result and raw_result['item']:
                            potential_items.append(raw_result['item'])
                        if potential_items:
                            items_to_save = potential_items
                        elif _looks_like_content(raw_result):
                            items_to_save = [raw_result]

                        new_requests_data = raw_result.get('new_requests') if isinstance(raw_result, dict) else None
                        if new_requests_data:
                            if isinstance(new_requests_data, (list, tuple)):
                                new_requests.extend(new_requests_data)
                            else:
                                new_requests.append(new_requests_data)
                    elif _looks_like_content(raw_result):
                        items_to_save = [raw_result]

                    # Schedule any newly discovered requests first
                    scheduled_requests = 0
                    for entry in new_requests:
                        try:
                            if entry is None:
                                continue
                            if hasattr(entry, 'url') and hasattr(entry, 'callback'):
                                # Assume it's a Scrapy Request
                                entry.meta.setdefault('adapter', adapter_local)
                                req = entry
                            else:
                                if isinstance(entry, dict):
                                    target_url = entry.get('url')
                                    if not target_url:
                                        continue
                                    callback = entry.get('callback', self.parse)
                                    err_callback = entry.get('errback', self.errback)
                                    dont_filter = entry.get('dont_filter', False)
                                    meta = entry.get('meta', {}) or {}
                                    meta.setdefault('adapter', adapter_local)
                                    resolved_url = urljoin(response.url, target_url)
                                    req = scrapy.Request(
                                        url=resolved_url,
                                        callback=callback,
                                        errback=err_callback,
                                        dont_filter=dont_filter,
                                        meta=meta
                                    )
                                else:
                                    resolved_url = urljoin(response.url, str(entry))
                                    req = scrapy.Request(
                                        url=resolved_url,
                                        callback=self.parse,
                                        errback=self.errback,
                                        dont_filter=False,
                                        meta={'adapter': adapter_local}
                                    )

                            if self.storage.is_url_visited(req.url):
                                continue

                            scheduled_requests += 1
                            yield req
                        except Exception as request_error:
                            logger.warning(f"Failed to schedule follow-up request from {response.url}: {request_error}")

                        if scheduled_requests and not items_to_save and not simple_output_flag:
                            print(f"[INFO] Discovered {scheduled_requests} new URLs from {response.url}")

                    if not items_to_save:
                        # Show why we're skipping (make reasons visible) only if we didn't enqueue new work
                        if not scheduled_requests:
                            if not simple_output_flag:
                                if status == 200:
                                    print(f"[WARN] No content extracted from {response.url}")
                                elif status == 404:
                                    print(f"[SKIP] Skipped (404 Not Found): {response.url}")
                                else:
                                    print(f"[SKIP] Skipped ({status}): {response.url}")
                            self.skipped_count += 1
                        return

                    for content_data in items_to_save:
                        if not isinstance(content_data, dict):
                            logger.warning(f"Unexpected content type from {response.url}: {type(content_data)}")
                            continue

                        if not adapter_local.validate_content(content_data):
                            if not simple_output_flag:
                                print(f"[ERROR] Invalid content from {response.url}")
                            logger.warning(f"Invalid content from {response.url}")
                            self.error_count += 1
                            continue

                        raw_title = content_data.get('title', '')
                        raw_content = content_data.get('content', '')
                        content_hash = compute_content_hash(raw_title, raw_content)

                        metadata = content_data.get('metadata') or {}
                        if not isinstance(metadata, dict):
                            metadata = {'value': metadata}

                        sanitized_metadata = {}
                        for meta_key, meta_value in metadata.items():
                            if isinstance(meta_value, str):
                                sanitized_metadata[meta_key] = clean_text(meta_value)
                            elif isinstance(meta_value, (int, float, bool)) or meta_value is None:
                                sanitized_metadata[meta_key] = meta_value
                            else:
                                sanitized_metadata[meta_key] = clean_text(str(meta_value))

                        storage_data = {
                            'id': content_data.get('id', f"{adapter_local.source_name}_{int(time.time())}_{self.scraped_count}"),
                            'source': adapter_local.source_name,
                            'url': adapter_local.normalize_url(content_data['url']),
                            'title': clean_text(raw_title),
                            'content': clean_text(raw_content),
                            'content_type': content_data['content_type'],
                            'metadata': sanitized_metadata,
                            'language': content_data.get('language') or adapter_local.detect_language(
                                f"{raw_title} {raw_content}"
                            ),
                            'retrieved_at': datetime.now().isoformat(),
                            'content_hash': content_hash
                        }

                        if self.storage.save_content(storage_data):
                            self.scraped_count += 1
                            title_preview = storage_data['title'] if len(storage_data['title']) <= 100 else storage_data['title'][:97] + '...'
                            if simple_output_flag:
                                print(f"[OK] Saved #{self.scraped_count}: {title_preview}")
                            else:
                                print(f"\n{'='*80}")
                                print(f"[OK] SAVED #{self.scraped_count}: {title_preview}")
                                print(f"   URL: {storage_data['url']}")
                                print(f"{'='*80}\n")
                            logger.info(f"[OK] SAVED #{self.scraped_count}: {title_preview} | {storage_data['url']}")

                            last_id_value = sanitized_metadata.get('question_id') or sanitized_metadata.get('hadith_number') or sanitized_metadata.get('sequence')
                            try:
                                last_id_value = int(str(last_id_value)) if last_id_value is not None and str(last_id_value).isdigit() else None
                            except Exception:
                                last_id_value = None

                            self.storage.update_resume_state(
                                adapter_local.source_name,
                                last_url=storage_data['url'],
                                last_id=last_id_value,
                                status='running'
                            )
                        else:
                            self.skipped_count += 1
                            if not simple_output_flag:
                                print(f"[SKIP] Skipped (duplicate): {storage_data['url']}")

                        now = time.time()
                        if now - progress_last['t'] >= 3 and not simple_output_flag:
                            print(f"\n>> PROGRESS: Saved: {self.scraped_count} | Skipped: {self.skipped_count} | Errors: {self.error_count}\n")
                            progress_last['t'] = now
                except Exception as e:
                    logger.error(f"Error parsing {response.url}: {e}", exc_info=True)
                    self.error_count += 1

            def errback(self, failure):
                self.error_count += 1
                logger.error(f"Request failed: {failure.request.url} - {failure.value}")

            def closed(self, reason):
                print(f"\n{'='*80}")
                print(f">> SCRAPING COMPLETE")
                print(f"{'='*80}")
                print(f"[OK] Saved: {self.scraped_count} items")
                print(f"    Skipped: {self.skipped_count} items")
                print(f"    Errors: {self.error_count} items")
                print(f"{'='*80}\n")
                logger.info(f"Spider closed: {reason}")
                logger.info(
                    f"Stats - Scraped: {self.scraped_count}, Skipped: {self.skipped_count}, Errors: {self.error_count}"
                )
                engine_self.storage.update_resume_state(
                    adapter.source_name,
                    status='completed' if reason == 'finished' else 'paused'
                )

        # Create crawler process
        process = CrawlerProcess(settings)
        process.crawl(ScraperSpider)
        
        # Start crawling
        process.start()
        
        logger.info(f"Completed scrape for {adapter.source_name}")
    
    def get_stats(self):
        """Get scraping statistics."""
        return self.storage.get_stats()

