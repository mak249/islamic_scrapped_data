# 🕌 Islamic Data Pipeline (Unified)

**Structure**: Isolated Pipelines per Source.

## 📂 Project Architecture

```
web scraping/
├── pipelines/
│   ├── islamqa/          # ✅ Active
│   │   ├── scraper.py    # (Prev: max_throughput.py)
│   │   ├── processor.py  # (Prev: clean_and_separate.py)
│   │   ├── data.db       # Main Database
│   │   └── output/       # Final JSONL datasets
│   ├── vedkabhed/        # ✅ Active
│   │   ├── scraper.py    # Playwright Scraper
│   │   └── output/       # Vedkabhed Data
│   └── sunnah/           # 📦 Archive
│       └── output/       # Restored Sunnah Data
├── shared/               # Shared logic (Cleaners, Monitors)
└── legacy/               # Old scripts
```

## 🚀 Usage

### 1. IslamQA Pipeline
```powershell
# Scrape
python pipelines/islamqa/scraper.py auto +10000

# Clean & Export
python pipelines/islamqa/processor.py --all
```

### 2. Vedkabhed Pipeline
```powershell
# Scrape (Auto monitors speed/resources)
python pipelines/vedkabhed/scraper.py
```

### 3. Check Stats
```powershell
python pipelines/islamqa/stats.py
```

## 📊 Output Locations
- **IslamQA**: `pipelines/islamqa/output/`
- **Vedkabhed**: `pipelines/vedkabhed/output/`
- **Sunnah**: `pipelines/sunnah/output/`
