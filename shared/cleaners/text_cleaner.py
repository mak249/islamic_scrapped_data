import html
import re
from bs4 import BeautifulSoup

def clean_text(text: str) -> str:
    """
    Clean text without altering meaning (Vedkabhed-safe).
    - Removes HTML
    - Removes UI elements
    - Removes invisible unicode chars (U+200F etc)
    - Normalizes whitespace
    """
    if not text:
        return ""
    
    # Decode HTML
    text = html.unescape(text)
    
    # Remove HTML tags & UI
    if '<' in text and '>' in text:
        soup = BeautifulSoup(text, 'html.parser')
        # Remove UI elements
        for tag in soup(["script", "style", "nav", "aside", "footer", "header", "button", "form", "dialog"]):
            tag.decompose()
        text = soup.get_text(separator=' ', strip=True)
    
    # Remove invisible unicode characters (RLM, LRM, ZWJ, etc.)
    # U+200B to U+200F, U+202A to U+202E
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e]', '', text)
    
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()