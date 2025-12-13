FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright
RUN pip install playwright
RUN playwright install chromium
RUN playwright install-deps

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scraper files
COPY fast_scraper.py .
COPY SIMPLE_SCRAPER.py .
COPY view_data.py .

# Create directories
RUN mkdir -p training_data

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SCRAPY_SETTINGS_MODULE=fast_scraper

# Default command
CMD ["python", "fast_scraper.py"]
