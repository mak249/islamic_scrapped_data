#!/usr/bin/env python3
"""
Quick script to continue scraping from last scraped ID
"""
import sys
from fast_scraper import IslamQASpider, run_fast_scraper, export_training_data

# Get last scraped ID and calculate next range
db_path = "islamqa_fast.db"
last_id = IslamQASpider.get_last_scraped_id(db_path)
start_id = last_id + 1
end_id = start_id + 9999  # Scrape next 10,000 IDs

print(f"📊 Last scraped ID: {last_id}")
print(f"🚀 Starting scrape from ID {start_id} to {end_id}")
print(f"📈 Estimated remaining: ~{end_id - start_id + 1:,} IDs\n")

# Run scraper
run_fast_scraper(start_id, end_id)

# Export data
export_training_data()

print(f"\n✅ Scraping completed!")
print(f"📊 New last ID: {IslamQASpider.get_last_scraped_id(db_path)}")

