"""
Text cleaning utilities for extracting clean text from HTML.
"""

import html
import re
from bs4 import BeautifulSoup


def clean_text(text):
    """
    Clean HTML and extract plain text.
    
    Args:
        text: Raw HTML or text string
        
    Returns:
        Cleaned plain text string
    """
    if not text:
        return ""
    # Decode HTML entities first
    text = html.unescape(text)
    # Remove HTML tags using BeautifulSoup
    soup = BeautifulSoup(text, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    # Clean up whitespace and normalize
    text = re.sub(r'\s+', ' ', text).strip()
    # Decode again in case BeautifulSoup didn't catch everything
    text = html.unescape(text)
    return text


def contains_html(text):
    """
    Check if text contains HTML tags.
    
    Args:
        text: Text to check
        
    Returns:
        True if HTML tags are present, False otherwise
    """
    if not text:
        return False
    return bool(re.search(r'<[^>]+>', text))


def normalize_text(text):
    """
    Normalize text for deduplication (lowercase, collapse whitespace).
    
    Args:
        text: Text to normalize
        
    Returns:
        Normalized text string
    """
    if not text:
        return ""
    # Convert to lowercase
    text = text.lower()
    # Collapse all whitespace to single spaces
    text = re.sub(r'\s+', ' ', text)
    # Strip leading/trailing whitespace
    return text.strip()

