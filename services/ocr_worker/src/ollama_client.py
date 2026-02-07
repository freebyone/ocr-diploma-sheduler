import base64
import json
import logging
import time
from typing import Dict, Any
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class OllamaOCRClient:
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "deepseek-ocr"):
        self.base_url = base_url
        self.model = model
        self.timeout = 600  # 10 минут
    
    def _encode_image_to_base64(self, image_data: bytes) -> str:
        """Кодировать изображение в base64"""
        return base64.b64encode(image_data).decode('utf-8')
    
    def check_health(self) -> bool:
        """Проверить доступность Ollama сервера"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            if response.status_code == 200:
                models = response.json().get('models', [])
                has_deepseek = any(m.get('name') == self.model for m in models)
                if has_deepseek:
                    logger.info(f"✓ Ollama доступен, модель {self.model} найдена")
                else:
                    logger.warning(f"⚠ Ollama доступен, но модель {self.model} не найдена")
                return True
            return False
        except Exception as e:
            logger.error(f"✗ Не удалось подключиться к Ollama: {e}")
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout))
    )
    def process_image(self, image_data: bytes, prompt_type: str = "markdown") -> Dict[str, Any]:
        """
        Обработать изображение через DeepSeek OCR
        
        prompt_type: "markdown" (по умолчанию) или "text"
        """
        base64_image = self._encode_image_to_base64(image_data)
        
        # Используем промпт для markdown из вашего рабочего скрипта
        if prompt_type == "markdown":
            prompt = "<|grounding|>Convert the document to markdown."
        else:
            prompt = "<image>\nFree OCR."
        
        payload = {
            "model": self.model,
            "prompt": prompt,
            "images": [base64_image],
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 8192,  # Увеличиваем для длинных документов
                "top_p": 0.9,
                "top_k": 40
            }
        }
        
        try:
            logger.debug(f"Отправка запроса к Ollama (prompt: {prompt_type})...")
            start_time = time.time()
            
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=self.timeout
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                
                formatted_result = {
                    "success": True,
                    "model": result.get("model", self.model),
                    "response": result.get("response", ""),
                    "prompt_type": prompt_type,
                    "metrics": {
                        "total_duration_sec": result.get("total_duration", 0) / 1e9,
                        "load_duration_ns": result.get("load_duration", 0),
                        "prompt_eval_count": result.get("prompt_eval_count", 0),
                        "eval_count": result.get("eval_count", 0),
                        "eval_duration_ns": result.get("eval_duration", 0),
                        "response_time_sec": response_time
                    },
                    "completion_info": {
                        "done": result.get("done", True),
                        "done_reason": result.get("done_reason", "stop")
                    }
                }
                
                logger.info(f"✓ OCR успешно выполнен за {response_time:.2f} секунд")
                logger.info(f"  Извлечено токенов: {result.get('eval_count', 0)}")
                logger.info(f"  Извлечено символов: {len(formatted_result['response'])}")
                
                return formatted_result
                
            else:
                error_msg = f"Ошибка HTTP {response.status_code}: {response.text}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg,
                    "status_code": response.status_code
                }
                
        except requests.exceptions.Timeout:
            error_msg = f"Таймаут при обработке изображения ({self.timeout} секунд)"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
            
        except requests.exceptions.ConnectionError:
            error_msg = f"Не удалось подключиться к Ollama серверу: {self.base_url}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }