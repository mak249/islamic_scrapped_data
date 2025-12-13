#!/usr/bin/env python3
"""
Data Viewer - View all scraped data from your databases
"""

import sqlite3
import json
import os
from datetime import datetime

def view_database_summary():
    """View summary of all databases"""
    print("📊 DATABASE SUMMARY")
    print("=" * 50)
    
    databases = [
        ('islamqa_improved.db', 'Improved Scraper'),
        ('islamqa_continuous.db', 'Continuous Scraper'),
        ('islamqa_data.db', 'Fixed Scraper')
    ]
    
    total_qa = 0
    for db_file, name in databases:
        if os.path.exists(db_file):
            try:
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM qa_pairs')
                count = cursor.fetchone()[0]
                total_qa += count
                print(f"{name}: {count:,} Q&A pairs")
                conn.close()
            except Exception as e:
                print(f"{name}: Error - {e}")
        else:
            print(f"{name}: Database not found")
    
    print(f"\nTotal Q&A pairs across all databases: {total_qa:,}")

def view_sample_data(db_file, limit=5):
    """View sample data from a specific database"""
    if not os.path.exists(db_file):
        print(f"❌ Database {db_file} not found")
        return
    
    print(f"\n📄 SAMPLE DATA FROM {db_file}")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM qa_pairs')
        total = cursor.fetchone()[0]
        print(f"Total Q&A pairs: {total:,}")
        
        # Get sample data
        cursor.execute('''
            SELECT question, answer, language, quality_score, url, scraped_at
            FROM qa_pairs 
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (limit,))
        
        rows = cursor.fetchall()
        
        for i, (question, answer, language, quality, url, scraped_at) in enumerate(rows, 1):
            print(f"\n--- Q&A Pair {i} ---")
            print(f"Question: {question[:80]}{'...' if len(question) > 80 else ''}")
            print(f"Answer: {answer[:80]}{'...' if len(answer) > 80 else ''}")
            print(f"Language: {language}")
            print(f"Quality Score: {quality:.2f}")
            print(f"URL: {url}")
            print(f"Scraped: {scraped_at}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error reading database: {e}")

def view_statistics(db_file):
    """View detailed statistics from a database"""
    if not os.path.exists(db_file):
        print(f"❌ Database {db_file} not found")
        return
    
    print(f"\n📈 STATISTICS FROM {db_file}")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Basic counts
        cursor.execute('SELECT COUNT(*) FROM qa_pairs')
        total = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT url) FROM qa_pairs')
        unique_urls = cursor.fetchone()[0]
        
        print(f"Total Q&A pairs: {total:,}")
        print(f"Unique URLs: {unique_urls:,}")
        print(f"Duplicates: {total - unique_urls:,}")
        
        # Language distribution
        cursor.execute('SELECT language, COUNT(*) FROM qa_pairs GROUP BY language')
        languages = cursor.fetchall()
        print(f"\nLanguage Distribution:")
        for lang, count in languages:
            print(f"  {lang}: {count:,} ({count/total*100:.1f}%)")
        
        # Quality distribution
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN quality_score >= 0.7 THEN 1 END) as high_quality,
                COUNT(CASE WHEN quality_score >= 0.4 AND quality_score < 0.7 THEN 1 END) as medium_quality,
                COUNT(CASE WHEN quality_score < 0.4 THEN 1 END) as low_quality,
                AVG(quality_score) as avg_quality
            FROM qa_pairs
        ''')
        quality_stats = cursor.fetchone()
        
        print(f"\nQuality Distribution:")
        print(f"  High Quality (0.7+): {quality_stats[0]:,} ({quality_stats[0]/total*100:.1f}%)")
        print(f"  Medium Quality (0.4-0.7): {quality_stats[1]:,} ({quality_stats[1]/total*100:.1f}%)")
        print(f"  Low Quality (<0.4): {quality_stats[2]:,} ({quality_stats[2]/total*100:.1f}%)")
        print(f"  Average Quality: {quality_stats[3]:.2f}")
        
        # Word count stats
        cursor.execute('SELECT AVG(word_count), MIN(word_count), MAX(word_count) FROM qa_pairs')
        word_stats = cursor.fetchone()
        print(f"\nWord Count Statistics:")
        print(f"  Average: {word_stats[0]:.0f} words")
        print(f"  Minimum: {word_stats[1]:,} words")
        print(f"  Maximum: {word_stats[2]:,} words")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error reading statistics: {e}")

def export_to_readable_format(db_file, output_file=None):
    """Export data to a readable text format"""
    if not os.path.exists(db_file):
        print(f"❌ Database {db_file} not found")
        return
    
    if output_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"data_export_{timestamp}.txt"
    
    print(f"\n📤 EXPORTING DATA TO {output_file}")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT question, answer, language, quality_score, url, scraped_at
            FROM qa_pairs 
            ORDER BY created_at DESC
        ''')
        
        rows = cursor.fetchall()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"ISLAMQA SCRAPED DATA EXPORT\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Q&A pairs: {len(rows):,}\n")
            f.write("=" * 80 + "\n\n")
            
            for i, (question, answer, language, quality, url, scraped_at) in enumerate(rows, 1):
                f.write(f"Q&A PAIR #{i}\n")
                f.write("-" * 40 + "\n")
                f.write(f"Question: {question}\n\n")
                f.write(f"Answer: {answer}\n\n")
                f.write(f"Language: {language}\n")
                f.write(f"Quality Score: {quality:.2f}\n")
                f.write(f"URL: {url}\n")
                f.write(f"Scraped: {scraped_at}\n")
                f.write("\n" + "=" * 80 + "\n\n")
        
        print(f"✅ Exported {len(rows):,} Q&A pairs to {output_file}")
        conn.close()
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")

def search_data(db_file, search_term, limit=10):
    """Search for specific content in the data"""
    if not os.path.exists(db_file):
        print(f"❌ Database {db_file} not found")
        return
    
    print(f"\n🔍 SEARCHING FOR: '{search_term}'")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT question, answer, language, quality_score, url
            FROM qa_pairs 
            WHERE question LIKE ? OR answer LIKE ?
            ORDER BY quality_score DESC
            LIMIT ?
        ''', (f'%{search_term}%', f'%{search_term}%', limit))
        
        rows = cursor.fetchall()
        
        if not rows:
            print("No results found")
            return
        
        print(f"Found {len(rows)} results:")
        
        for i, (question, answer, language, quality, url) in enumerate(rows, 1):
            print(f"\n--- Result {i} ---")
            print(f"Question: {question[:100]}{'...' if len(question) > 100 else ''}")
            print(f"Answer: {answer[:100]}{'...' if len(answer) > 100 else ''}")
            print(f"Language: {language}, Quality: {quality:.2f}")
            print(f"URL: {url}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error searching data: {e}")

def main():
    """Main function with interactive menu"""
    print("🔍 ISLAMQA DATA VIEWER")
    print("=" * 50)
    
    while True:
        print("\nChoose an option:")
        print("1. View database summary")
        print("2. View sample data from improved database")
        print("3. View sample data from continuous database")
        print("4. View sample data from fixed database")
        print("5. View statistics from improved database")
        print("6. View statistics from continuous database")
        print("7. View statistics from fixed database")
        print("8. Export data to readable format")
        print("9. Search data")
        print("0. Exit")
        
        choice = input("\nEnter your choice (0-9): ").strip()
        
        if choice == "0":
            print("👋 Goodbye!")
            break
        elif choice == "1":
            view_database_summary()
        elif choice == "2":
            view_sample_data('islamqa_improved.db', 10)
        elif choice == "3":
            view_sample_data('islamqa_continuous.db', 10)
        elif choice == "4":
            view_sample_data('islamqa_data.db', 10)
        elif choice == "5":
            view_statistics('islamqa_improved.db')
        elif choice == "6":
            view_statistics('islamqa_continuous.db')
        elif choice == "7":
            view_statistics('islamqa_data.db')
        elif choice == "8":
            db_file = input("Enter database file (islamqa_improved.db, islamqa_continuous.db, or islamqa_data.db): ").strip()
            if db_file in ['islamqa_improved.db', 'islamqa_continuous.db', 'islamqa_data.db']:
                export_to_readable_format(db_file)
            else:
                print("Invalid database file")
        elif choice == "9":
            db_file = input("Enter database file (islamqa_improved.db, islamqa_continuous.db, or islamqa_data.db): ").strip()
            if db_file in ['islamqa_improved.db', 'islamqa_continuous.db', 'islamqa_data.db']:
                search_term = input("Enter search term: ").strip()
                if search_term:
                    search_data(db_file, search_term)
            else:
                print("Invalid database file")
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
