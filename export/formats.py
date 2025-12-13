"""
Export system for AI training formats.
Supports filtering by source, content_type, and language.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class TrainingDataExporter:
    """
    Export scraper data to various AI training formats.
    """
    
    def __init__(self, output_dir: str = "training_data"):
        """
        Initialize exporter.
        
        Args:
            output_dir: Directory for exported files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def export_all_formats(self, content_list: List[Dict[str, Any]], 
                          prefix: str = "islamic_data") -> Dict[str, str]:
        """
        Export content to all supported formats.
        
        Args:
            content_list: List of content dictionaries from storage
            prefix: Prefix for output filenames
            
        Returns:
            Dictionary mapping format names to file paths
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        exported_files = {}
        
        # Export in all formats
        exported_files['json'] = self.export_json(content_list, f"{prefix}_{timestamp}.json")
        exported_files['chatgpt'] = self.export_chatgpt(content_list, f"{prefix}_chatgpt_{timestamp}.jsonl")
        exported_files['llama'] = self.export_llama(content_list, f"{prefix}_llama_{timestamp}.jsonl")
        exported_files['alpaca'] = self.export_alpaca(content_list, f"{prefix}_alpaca_{timestamp}.json")
        exported_files['rag'] = self.export_rag(content_list, f"{prefix}_rag_{timestamp}.jsonl")
        exported_files['text'] = self.export_text(content_list, f"{prefix}_{timestamp}.txt")
        
        logger.info(f"Exported {len(content_list)} items in 6 formats")
        return exported_files
    
    def export_json(self, content_list: List[Dict[str, Any]], filename: str) -> str:
        """
        Export to simple JSON format.
        
        Args:
            content_list: List of content dictionaries
            filename: Output filename
            
        Returns:
            Path to exported file
        """
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(content_list, f, ensure_ascii=False, indent=2)
        return str(filepath)
    
    def export_chatgpt(self, content_list: List[Dict[str, Any]], filename: str) -> str:
        """
        Export to ChatGPT conversation format (JSONL).
        
        Args:
            content_list: List of content dictionaries
            filename: Output filename
            
        Returns:
            Path to exported file
        """
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for content in content_list:
                # Extract question and answer for Q&A content
                question, answer = self._extract_qa(content)
                
                conversation = {
                    "messages": [
                        {"role": "user", "content": question},
                        {"role": "assistant", "content": answer}
                    ],
                    "metadata": {
                        "id": content.get('id'),
                        "source": content.get('source'),
                        "url": content.get('url'),
                        "content_type": content.get('content_type'),
                        "language": content.get('language'),
                        "metadata": content.get('metadata', {})
                    }
                }
                f.write(json.dumps(conversation, ensure_ascii=False) + '\n')
        
        return str(filepath)
    
    def export_llama(self, content_list: List[Dict[str, Any]], filename: str) -> str:
        """
        Export to LLaMA instruction format (JSONL).
        
        Args:
            content_list: List of content dictionaries
            filename: Output filename
            
        Returns:
            Path to exported file
        """
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for content in content_list:
                question, answer = self._extract_qa(content)
                
                instruction_data = {
                    "instruction": f"Answer this Islamic question: {question}",
                    "input": "",
                    "output": answer,
                    "metadata": {
                        "id": content.get('id'),
                        "source": content.get('source'),
                        "language": content.get('language'),
                        "content_type": content.get('content_type')
                    }
                }
                f.write(json.dumps(instruction_data, ensure_ascii=False) + '\n')
        
        return str(filepath)
    
    def export_alpaca(self, content_list: List[Dict[str, Any]], filename: str) -> str:
        """
        Export to Alpaca format (JSON array).
        
        Args:
            content_list: List of content dictionaries
            filename: Output filename
            
        Returns:
            Path to exported file
        """
        filepath = self.output_dir / filename
        
        alpaca_data = []
        for content in content_list:
            question, answer = self._extract_qa(content)
            
            alpaca_entry = {
                "instruction": f"Answer this Islamic question: {question}",
                "input": "",
                "output": answer,
                "source": content.get('source'),
                "language": content.get('language'),
                "content_type": content.get('content_type')
            }
            alpaca_data.append(alpaca_entry)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(alpaca_data, f, ensure_ascii=False, indent=2)
        
        return str(filepath)
    
    def export_rag(self, content_list: List[Dict[str, Any]], filename: str) -> str:
        """
        Export to RAG document format (JSONL).
        
        Args:
            content_list: List of content dictionaries
            filename: Output filename
            
        Returns:
            Path to exported file
        """
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for content in content_list:
                document = {
                    "id": content.get('id'),
                    "content": content.get('content', ''),
                    "metadata": {
                        "title": content.get('title'),
                        "source": content.get('source'),
                        "url": content.get('url'),
                        "content_type": content.get('content_type'),
                        "language": content.get('language'),
                        "retrieved_at": content.get('retrieved_at'),
                        **content.get('metadata', {})
                    }
                }
                f.write(json.dumps(document, ensure_ascii=False) + '\n')
        
        return str(filepath)
    
    def export_text(self, content_list: List[Dict[str, Any]], filename: str) -> str:
        """
        Export to human-readable text format.
        
        Args:
            content_list: List of content dictionaries
            filename: Output filename
            
        Returns:
            Path to exported file
        """
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("ISLAMIC DATA EXPORT\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total items: {len(content_list):,}\n")
            f.write("=" * 80 + "\n\n")
            
            for i, content in enumerate(content_list, 1):
                f.write(f"Item #{i}\n")
                f.write("-" * 40 + "\n")
                f.write(f"Source: {content.get('source', 'Unknown')}\n")
                f.write(f"Type: {content.get('content_type', 'Unknown')}\n")
                f.write(f"Title: {content.get('title', 'N/A')}\n\n")
                f.write(f"Content:\n{content.get('content', 'N/A')}\n\n")
                f.write(f"Language: {content.get('language', 'Unknown')}\n")
                f.write(f"URL: {content.get('url', 'N/A')}\n")
                f.write(f"Retrieved: {content.get('retrieved_at', 'N/A')}\n")
                f.write("\n" + "=" * 80 + "\n\n")
        
        return str(filepath)
    
    def _extract_qa(self, content: Dict[str, Any]) -> tuple:
        """
        Extract question and answer from content.
        Handles different content types and formats.
        
        Args:
            content: Content dictionary
            
        Returns:
            Tuple of (question, answer)
        """
        content_type = content.get('content_type', '')
        content_text = content.get('content', '')
        metadata = content.get('metadata', {})
        
        # For Q&A content, try to extract from metadata or content
        if content_type == 'q&a':
            # Check metadata first
            if 'question' in metadata and 'answer' in metadata:
                return metadata['question'], metadata['answer']
            
            # Try to parse from content (format: "Question: ...\n\nAnswer: ...")
            if 'Question:' in content_text and 'Answer:' in content_text:
                parts = content_text.split('Answer:')
                if len(parts) == 2:
                    question = parts[0].replace('Question:', '').strip()
                    answer = parts[1].strip()
                    return question, answer
        
        # For other content types, use title as question and content as answer
        question = content.get('title', '')
        answer = content_text
        
        return question, answer

