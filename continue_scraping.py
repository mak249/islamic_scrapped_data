#!/usr/bin/env python3
"""
Helper script to continue scraping from the last question_id.
Uses the unified storage system to resume scraping.
"""
import sys
from scrapers.core import CoreEngine
from scrapers.storage import UnifiedStorage
from scrapers.adapters.islamqa import IslamQAAdapter

def continue_scraping(start_id=None, end_id=None, batch_size=10000):
    """
    Continue scraping from the last scraped question_id.
    
    Args:
        start_id: Optional starting ID (overrides resume state)
        end_id: Optional ending ID (defaults to start_id + batch_size)
        batch_size: Number of IDs to scrape if end_id not specified
    """
    # Initialize storage and engine
    db_path = "islamic_data.db"
    storage = UnifiedStorage(db_path)
    engine = CoreEngine(db_path=db_path, default_delay=1.0)
    
    # Get last scraped ID
    resume_state = storage.get_resume_state('islamqa')
    if resume_state and resume_state.get('last_id'):
        last_id = resume_state['last_id']
        print(f"📊 Last scraped question_id: {last_id}")
    else:
        # Try to get from adapter
        last_id = IslamQAAdapter.get_last_scraped_id(storage)
        print(f"📊 Last scraped question_id (from content): {last_id}")
    
    # Determine start and end IDs
    if start_id is None:
        start_id = last_id + 1
    if end_id is None:
        end_id = start_id + batch_size - 1
    
    print(f"🚀 Starting scrape from question_id {start_id} to {end_id}")
    print(f"📈 Will scrape ~{end_id - start_id + 1:,} question IDs\n")
    
    # Create adapter (it will auto-resume from storage if start_id is before last_id)
    adapter = IslamQAAdapter(start_id=start_id, end_id=end_id, storage=storage)
    
    # Scrape
    engine.scrape_site(
        adapter=adapter,
        concurrent_requests=8,
        download_delay=1.0,
        simple_output=True
    )
    
    # Show final status
    final_state = storage.get_resume_state('islamqa')
    if final_state:
        print(f"\n✅ Scraping completed!")
        print(f"📊 Final question_id: {final_state.get('last_id', 'N/A')}")
        print(f"📊 Status: {final_state.get('status', 'N/A')}")
    else:
        final_id = IslamQAAdapter.get_last_scraped_id(storage)
        print(f"\n✅ Scraping completed!")
        print(f"📊 Final question_id: {final_id}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Continue scraping from last question_id',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Continue from last ID (scrape next 10,000)
  python continue_scraping.py
  
  # Continue with custom range
  python continue_scraping.py --start-id 50000 --end-id 60000
  
  # Continue with custom batch size
  python continue_scraping.py --batch-size 5000
        """
    )
    
    parser.add_argument('--start-id', type=int,
                       help='Starting question_id (overrides resume state)')
    parser.add_argument('--end-id', type=int,
                       help='Ending question_id')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='Number of IDs to scrape if end-id not specified (default: 10000)')
    
    args = parser.parse_args()
    
    continue_scraping(
        start_id=args.start_id,
        end_id=args.end_id,
        batch_size=args.batch_size
    )
