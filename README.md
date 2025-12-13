# 🕌 IslamQA Data Pipeline

A comprehensive data pipeline for scraping, cleaning, storing, and exporting Islamic Q&A data from islamqa.info for AI assistant training.

## 🚀 Features

### **Data Scraping**
- Intelligent web scraping with respect for rate limits
- Multi-language support (English/Arabic)
- Robust error handling and retry mechanisms
- Category-based data collection

### **Data Cleaning**
- Text normalization and HTML entity decoding
- Quality scoring and validation
- Duplicate detection and removal
- Language detection and categorization

### **Data Storage**
- SQLite database with optimized schema
- Full-text search capabilities
- Metadata preservation
- Automated backups

### **AI Training Export**
- Multiple format support (ChatGPT, LLaMA, Alpaca, RAG)
- Training/validation/test splits
- Quality-based filtering
- Language-specific exports

## 📁 Project Structure

```
web scraping/
├── islamqa.py                 # Main web scraper
├── data_cleaner.py           # Data cleaning and preprocessing
├── data_storage.py           # Database storage system
├── ai_training_exporter.py   # Export utilities for AI training
├── complete_pipeline.py      # Integrated pipeline
├── config.json              # Configuration file
├── requirements.txt         # Python dependencies
├── example_usage.py         # Usage examples
└── README.md               # This file
```

## 🛠️ Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   # Or run the installation script:
   python install_deps.py
   ```

2. **Verify installation:**
   ```bash
   python main.py status
   ```

3. **If you get import errors in your IDE:**
   - Make sure your IDE is using the same Python environment where you installed the packages
   - Try: `python -m pip install pyyaml` if yaml import fails

## 🎯 Quick Start

### **Check Status**
```bash
python main.py status
```

### **Scrape Data**
```bash
# Scrape islamqa.info
python main.py scrape --site islamqa --start-id 1 --end-id 1000

# Continue from where you left off (resume support)
python main.py scrape --site islamqa --start-id 199428 --end-id 200000
```

### **Export for AI Training**
```bash
# Export all Q&A data in all formats
python main.py export --content-type "q&a"

# Export specific source
python main.py export --source islamqa --limit 5000

# Export in specific language
python main.py export --language english
```

### **Migrate Existing Data**
```bash
# Migrate from old islamqa database
python main.py migrate --old-db islamqa_fast.db

# Migrate from all found databases
python main.py migrate --all-dbs
```

## 📊 Data Flow

```
Raw Web Data → Cleaning → Database → AI Training Formats
     ↓              ↓         ↓            ↓
islamqa.info → Text Processing → SQLite → JSON/CSV/TXT
```

## 🔧 Configuration

Edit `config.json` to customize the pipeline:

```json
{
  "scraping": {
    "max_questions_per_category": 50,
    "max_categories": 10,
    "delay_between_requests": 1.5
  },
  "cleaning": {
    "min_question_length": 15,
    "min_answer_length": 100,
    "min_quality_score": 0.4
  },
  "export": {
    "formats": ["chatgpt", "llama", "alpaca", "rag"],
    "create_splits": true
  }
}
```

## 📈 Output Formats

### **ChatGPT Format**
```json
{
  "messages": [
    {"role": "user", "content": "Question..."},
    {"role": "assistant", "content": "Answer..."}
  ]
}
```

### **LLaMA Format**
```json
{
  "instruction": "Answer this Islamic question...",
  "input": "",
  "output": "Answer text..."
}
```

### **RAG Format**
```json
{
  "id": "qa_123",
  "content": "Question: ...\n\nAnswer: ...",
  "metadata": {...}
}
```

## 🗄️ Database Schema

### **qa_pairs Table**
- `id`: Unique identifier
- `question`: Question text
- `answer`: Answer text
- `language`: Detected language
- `quality_score`: Quality rating (0-1)
- `word_count`: Total word count
- `scraped_at`: Scraping timestamp

### **categories Table**
- `qa_id`: Foreign key to qa_pairs
- `category`: Category name

### **sources Table**
- `qa_id`: Foreign key to qa_pairs
- `source`: Source reference

## 🔍 Usage Examples

### **Search Database**
```python
from data_storage import IslamQADatabase

db = IslamQADatabase()

# Search by keyword
results = db.search_qa_pairs(query="prayer", limit=10)

# Filter by language
english_qa = db.search_qa_pairs(language="english", limit=50)

# Filter by quality
high_quality = db.search_qa_pairs(min_quality=0.8, limit=20)
```

### **Export Specific Data**
```python
from ai_training_exporter import AITrainingExporter

exporter = AITrainingExporter()

# Export high-quality English data
high_quality_data = db.search_qa_pairs(
    language="english", 
    min_quality=0.7, 
    limit=1000
)

exporter.export_for_chatgpt(high_quality_data)
```

## 📊 Quality Metrics

The pipeline includes several quality metrics:

- **Quality Score**: 0-1 based on completeness and content quality
- **Word Count**: Total words in Q&A pair
- **Language Detection**: Automatic Arabic/English detection
- **Category Coverage**: Number of categories represented
- **Source References**: Presence of Islamic sources

## 🚨 Best Practices

1. **Respectful Scraping**: Built-in delays to avoid overwhelming the server
2. **Data Validation**: Multiple quality checks and filters
3. **Backup Strategy**: Automatic database backups
4. **Error Handling**: Comprehensive error recovery
5. **Modular Design**: Each component can be used independently

## 🔧 Troubleshooting

### **Common Issues**

1. **No data scraped**: Check internet connection and website availability
2. **Database errors**: Ensure SQLite is properly installed
3. **Export failures**: Check output directory permissions
4. **Memory issues**: Reduce batch sizes in configuration

### **Debug Mode**
```bash
# Run with verbose output
python complete_pipeline.py --mode test --questions 5 --categories 1
```

## 📝 License

This project is for educational and research purposes. Please respect the terms of service of islamqa.info when using this tool.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review the example files
3. Open an issue with detailed error information

---

**Happy Training! 🚀**
