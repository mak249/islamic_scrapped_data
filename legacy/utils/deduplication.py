"""
Content hash-based deduplication engine.
"""

import hashlib
from typing import Optional
from utils.text_cleaner import normalize_text


def compute_content_hash(title: str, content: str) -> str:
    """
    Compute SHA256 hash of normalized content for deduplication.
    
    Args:
        title: Content title
        content: Content body
        
    Returns:
        Hex digest of content hash
    """
    # Normalize and combine title + content
    normalized = normalize_text(f"{title}\n{content}")
    # Compute SHA256 hash
    hash_obj = hashlib.sha256(normalized.encode('utf-8'))
    return hash_obj.hexdigest()


def compute_url_hash(url: str) -> str:
    """
    Compute SHA256 hash of normalized URL.
    
    Args:
        url: URL string
        
    Returns:
        Hex digest of URL hash
    """
    normalized = normalize_text(url)
    hash_obj = hashlib.sha256(normalized.encode('utf-8'))
    return hash_obj.hexdigest()

