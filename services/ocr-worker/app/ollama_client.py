import requests
import base64
import time
import json
from typing import Dict, Optional, Any
from app.config import config
import logging

logger = logging.getLogger(__name__)

class OllamaClient:
    def __init__(self):
        self.base_url = config.OLLAMA_HOST
        self.model = config.OLLAMA_MODEL
        self.timeout = config.OLLAMA_TIMEOUT
        
    def check_connection(self):
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            if response.status_code == 200:
                models = response.json().get('models', [])
                model_exists = any(m['name'] == self.model for m in models)
                if not model_exists:
                    logger.warning(f"Model {self.model} not found in Ollama")
                    return False
                return True
            return False
        except Exception as e:
            logger.error(f"Ollama connection error: {e}")
            return False
    
    def encode_image_to_base64(self, image_path: str) -> Optional[str]:
        try:
            with open(image_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode('utf-8')
        except Exception as e:
            logger.error(f"Error encoding image to base64: {e}")
            return None
    
    def process_image(self, image_path: str, prompt_type: str = "text") -> Dict[str, Any]:
        start_time = time.time()
        
        prompts = {
            "text": "<image>\nExtract all text from this image.",
            "markdown": "<|grounding|>Convert the document to markdown format.",
            "table": "<|grounding|>Extract all tables from this image in structured format.",
            "details": "<|grounding|>Provide detailed description of this image including text, structure, and layout."
        }
        
        prompt = prompts.get(prompt_type, prompts["text"])
        
        base64_image = self.encode_image_to_base64(image_path)
        if not base64_image:
            return {
                "success": False,
                "error": "Failed to encode image"
            }
        
        url = f"{self.base_url}/api/generate"
        payload = {
            "model": self.model,
            "prompt": prompt,
            "images": [base64_image],
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 4096
            }
        }
        
        try:
            logger.info(f"Sending image to Ollama: {image_path}, prompt: {prompt_type}")
            
            response = requests.post(url, json=payload, timeout=self.timeout)
            
            if response.status_code == 200:
                result = response.json()
                processing_time = time.time() - start_time
                
                logger.info(f"Ollama response received in {processing_time:.2f}s")
                
                return {
                    "success": True,
                    "response": result.get('response', ''),
                    "model": result.get('model', self.model),
                    "processing_time": processing_time,
                    "tokens_count": result.get('eval_count', 0),
                    "total_duration": result.get('total_duration', 0),
                    "done_reason": result.get('done_reason', ''),
                    "metadata": {
                        "prompt_type": prompt_type,
                        "image_size": len(base64_image),
                        "status_code": response.status_code
                    }
                }
            else:
                logger.error(f"Ollama API error: {response.status_code} - {response.text}")
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text[:200]}",
                    "status_code": response.status_code
                }
                
        except requests.exceptions.Timeout:
            logger.error(f"Ollama request timeout after {self.timeout}s")
            return {
                "success": False,
                "error": f"Request timeout after {self.timeout}s"
            }
        except requests.exceptions.ConnectionError:
            logger.error("Cannot connect to Ollama")
            return {
                "success": False,
                "error": "Cannot connect to Ollama service"
            }
        except Exception as e:
            logger.error(f"Unexpected error processing image: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def batch_process(self, image_paths: list, prompt_type: str = "text"):
        """Обработать несколько изображений"""
        results = []
        for i, image_path in enumerate(image_paths, 1):
            logger.info(f"Processing image {i}/{len(image_paths)}: {image_path}")
            result = self.process_image(image_path, prompt_type)
            results.append({
                "image_path": image_path,
                "result": result
            })
            time.sleep(0.5)
        
        return results

ollama_client = OllamaClient()