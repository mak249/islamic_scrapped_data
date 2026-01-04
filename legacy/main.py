"""
CLI entry point for Universal Islamic Data Scraper.
"""

import argparse
try:
    import yaml  # type: ignore[import]
except Exception:
    yaml = None  # Fallback: run with built-in defaults if YAML not available
import logging
from pathlib import Path
from typing import Optional

from scrapers.core import CoreEngine
from scrapers.storage import UnifiedStorage
from scrapers.adapters.islamqa import IslamQAAdapter
from export.formats import TrainingDataExporter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/scraper.yaml") -> dict:
    """Load configuration from YAML file."""
    if yaml is None:
        logger.warning("PyYAML not installed; using default config")
        return {}
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Config file not found: {config_path}, using defaults")
        return {}


def load_sites_config(config_path: str = "config/sites.yaml") -> dict:
    """Load sites configuration from YAML file."""
    if yaml is None:
        logger.warning("PyYAML not installed; using default sites config")
        return {}
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Sites config not found: {config_path}")
        return {}


def cmd_scrape(args):
    """Scrape a specific site."""
    logger.info(f"Starting scrape for site: {args.site}")
    
    # Load configs
    config = load_config()
    sites_config = load_sites_config()
    
    # Get site config
    site_config = sites_config.get('sites', {}).get(args.site, {})
    if not site_config or not site_config.get('enabled', True):
        logger.error(f"Site {args.site} is not enabled or not found")
        return
    
    # Initialize core engine
    db_path = config.get('database', {}).get('path', 'islamic_data.db')
    default_delay = config.get('defaults', {}).get('download_delay', 1.0)
    user_agent = config.get('user_agent', 'IslamicDataScraper/1.0')
    
    engine = CoreEngine(
        db_path=db_path,
        default_delay=default_delay,
        user_agent=user_agent
    )
    
    # Initialize storage for adapter
    storage = UnifiedStorage(db_path)
    
    # Create adapter based on site
    site = args.site.lower()
    if site == 'islamqa':
        start_id = args.start_id if args.start_id else site_config.get('start_id', 1)
        end_id = args.end_id if args.end_id else site_config.get('end_id', 10000)
        adapter = IslamQAAdapter(start_id=start_id, end_id=end_id, storage=storage)
    elif site == 'sunnah':
        from scrapers.adapters.sunnah import SunnahAdapter
        adapter = SunnahAdapter(start_urls=site_config.get('start_urls'))
    elif site == 'islamweb':
        from scrapers.adapters.islamweb import IslamWebAdapter
        adapter = IslamWebAdapter(start_urls=site_config.get('start_urls'))
    elif site == 'shamela':
        from scrapers.adapters.shamela import ShamelaAdapter
        adapter = ShamelaAdapter(start_urls=site_config.get('start_urls'))
    elif site == 'sunnahonline':
        from scrapers.adapters.sunnahonline import SunnahOnlineAdapter
        adapter = SunnahOnlineAdapter(start_urls=site_config.get('start_urls'))
    elif site == 'ahadith':
        from scrapers.adapters.ahadith import AhadithAdapter
        adapter = AhadithAdapter(start_urls=site_config.get('start_urls'))
    elif site in ('sahih_bukhari', 'sahih-bukhari'):
        from scrapers.adapters.sahih_bukhari import SahihBukhariAdapter
        adapter = SahihBukhariAdapter(start_urls=site_config.get('start_urls'))
    elif site == 'darussalam':
        from scrapers.adapters.darussalam import DarussalamAdapter
        adapter = DarussalamAdapter(start_urls=site_config.get('start_urls'))
    elif site == 'salafipublications':
        from scrapers.adapters.salafipublications import SalafiPublicationsAdapter
        adapter = SalafiPublicationsAdapter(start_urls=site_config.get('start_urls'))
    elif site == 'abdurrahman':
        from scrapers.adapters.abdurrahman import AbdurrahmanAdapter
        adapter = AbdurrahmanAdapter(start_urls=site_config.get('start_urls'))
    else:
        logger.error(f"Adapter for {args.site} not yet implemented")
        logger.info("Available sites: islamqa, sunnah, islamweb, shamela, sunnahonline, ahadith, sahih_bukhari, darussalam, salafipublications, abdurrahman")
        return
    
    # Scrape
    # Compute effective settings (allow CLI overrides)
    if getattr(args, 'download_delay', None) is not None:
        download_delay = args.download_delay
    else:
        download_delay = site_config.get('download_delay', default_delay)

    if getattr(args, 'concurrency', None) is not None:
        concurrent_requests = args.concurrency
    else:
        concurrent_requests = site_config.get('concurrent_requests', 
                                              config.get('defaults', {}).get('concurrent_requests', 8))

    log_level = args.log_level if getattr(args, 'log_level', None) else config.get('logging', {}).get('level', 'INFO')

    # Fast mode tweaks
    disable_rate_limit = False
    if getattr(args, 'fast', False):
        download_delay = 0.0
        concurrent_requests = max(concurrent_requests, 24)
        disable_rate_limit = True
        log_level = 'WARNING'  # Suppress most Scrapy noise
    if getattr(args, 'no_rate_limit', False):
        disable_rate_limit = True
    
    # Auto-enable simple output for better UX
    simple_output = getattr(args, 'simple_output', True)  # Default to True
    
    engine.scrape_site(
        adapter=adapter,
        concurrent_requests=concurrent_requests,
        download_delay=download_delay,
        log_level=log_level,
        disable_rate_limit=disable_rate_limit,
        simple_output=simple_output,
        fast_mode=getattr(args, 'fast', False)
    )
    
    # Print stats
    stats = engine.get_stats()
    logger.info("\nScraping Statistics:")
    logger.info(f"Total records: {stats['total']}")
    logger.info(f"By source: {stats['by_source']}")


def cmd_export(args):
    """Export data to training formats."""
    logger.info("Exporting training data...")
    
    # Load config
    config = load_config()
    db_path = config.get('database', {}).get('path', 'islamic_data.db')
    output_dir = config.get('export', {}).get('output_dir', 'training_data')
    
    # Initialize storage and exporter
    storage = UnifiedStorage(db_path)
    exporter = TrainingDataExporter(output_dir=output_dir)
    
    # Query content with filters
    content_list = storage.query_content(
        source=args.source,
        content_type=args.content_type,
        language=args.language,
        limit=args.limit
    )
    
    if not content_list:
        logger.warning("No content found matching filters")
        return
    
    logger.info(f"Exporting {len(content_list)} items...")
    
    # Export
    prefix = args.prefix or "islamic_data"
    exported_files = exporter.export_all_formats(content_list, prefix=prefix)
    
    logger.info("\nExported files:")
    for format_name, filepath in exported_files.items():
        logger.info(f"  {format_name}: {filepath}")


def cmd_migrate(args):
    """Migrate existing data."""
    logger.info("Migrating existing data...")
    
    from migrate_existing import migrate_islamqa_db
    
    config = load_config()
    db_path = config.get('database', {}).get('path', 'islamic_data.db')
    
    storage = UnifiedStorage(db_path)
    
    if args.all_dbs:
        # Find all databases
        db_files = list(Path('.').glob('islamqa*.db'))
        logger.info(f"Found {len(db_files)} databases to migrate")
        
        for db_file in db_files:
            logger.info(f"\nMigrating {db_file}...")
            migrate_islamqa_db(str(db_file), storage)
    else:
        old_db = args.old_db or 'islamqa_fast.db'
        migrate_islamqa_db(old_db, storage)
    
    # Print stats
    stats = storage.get_stats()
    logger.info("\nMigration Statistics:")
    logger.info(f"Total records: {stats['total']}")
    logger.info(f"By source: {stats['by_source']}")


def cmd_status(args):
    """Show scraping status and statistics."""
    config = load_config()
    db_path = config.get('database', {}).get('path', 'islamic_data.db')
    
    storage = UnifiedStorage(db_path)
    stats = storage.get_stats()
    
    print("\n" + "=" * 60)
    print("SCRAPING STATUS")
    print("=" * 60)
    print(f"\nTotal Records: {stats['total']:,}")
    
    if stats['by_source']:
        print("\nBy Source:")
        for source, count in sorted(stats['by_source'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {source:20s}: {count:>8,}")
    
    if stats['by_content_type']:
        print("\nBy Content Type:")
        for ctype, count in sorted(stats['by_content_type'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {ctype:20s}: {count:>8,}")
    
    if stats['by_language']:
        print("\nBy Language:")
        for lang, count in sorted(stats['by_language'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {lang:20s}: {count:>8,}")
    
    # Show resume states
    print("\nResume States:")
    sites_config = load_sites_config()
    for site_name in sites_config.get('sites', {}).keys():
        resume_state = storage.get_resume_state(site_name)
        if resume_state:
            status = resume_state.get('status', 'unknown')
            last_id = resume_state.get('last_id', 'N/A')
            last_url = resume_state.get('last_url') or 'N/A'
            last_url_display = last_url[:50] + '...' if last_url != 'N/A' and len(last_url) > 50 else last_url
            print(f"  {site_name:20s}: {status:10s} (last_id: {last_id}, last_url: {last_url_display})")
        else:
            print(f"  {site_name:20s}: not started")
    
    print("\n" + "=" * 60)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Universal Islamic Data Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape islamqa.info
  python main.py scrape --site islamqa --start-id 1 --end-id 1000
  
  # Export all Q&A data
  python main.py export --content-type "q&a"
  
  # Export specific source
  python main.py export --source islamqa --format chatgpt
  
  # Migrate existing data
  python main.py migrate --old-db islamqa_fast.db
  
  # Show status
  python main.py status
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape a site')
    scrape_parser.add_argument('--site', type=str, required=True,
                              help='Site to scrape (e.g., islamqa)')
    scrape_parser.add_argument('--start-id', type=int,
                              help='Starting ID (for islamqa)')
    scrape_parser.add_argument('--end-id', type=int,
                              help='Ending ID (for islamqa)')
    scrape_parser.add_argument('--download-delay', type=float,
                              help='Seconds between requests (overrides config)')
    scrape_parser.add_argument('--concurrency', type=int,
                              help='Total concurrent requests (overrides config)')
    scrape_parser.add_argument('--fast', action='store_true',
                              help='Fast mode: 0 delay, high concurrency, no rate limiting')
    scrape_parser.add_argument('--no-rate-limit', action='store_true',
                              help='Disable internal rate limiter (still subject to site limits)')
    scrape_parser.add_argument('--log-level', type=str, choices=['DEBUG','INFO','WARNING','ERROR'],
                              help='Log level for this run')
    scrape_parser.add_argument('--simple-output', action='store_true',
                              help='Cleaner terminal output (hide Scrapy noise, show concise progress)')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data to training formats')
    export_parser.add_argument('--source', type=str,
                              help='Filter by source')
    export_parser.add_argument('--content-type', type=str,
                              help='Filter by content type (q&a, hadith, article, metadata)')
    export_parser.add_argument('--language', type=str,
                              help='Filter by language')
    export_parser.add_argument('--limit', type=int,
                              help='Limit number of records')
    export_parser.add_argument('--prefix', type=str,
                              help='Prefix for output filenames')
    
    # Migrate command
    migrate_parser = subparsers.add_parser('migrate', help='Migrate existing data')
    migrate_parser.add_argument('--old-db', type=str,
                               help='Path to old database')
    migrate_parser.add_argument('--all-dbs', action='store_true',
                               help='Migrate from all found databases')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show scraping status')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute command
    if args.command == 'scrape':
        cmd_scrape(args)
    elif args.command == 'export':
        cmd_export(args)
    elif args.command == 'migrate':
        cmd_migrate(args)
    elif args.command == 'status':
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

