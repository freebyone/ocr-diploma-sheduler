import sys
import os
sys.path.append('/app')

from minio_client import MinIOClient
from ollama_client import OllamaOCRClient
import base64

# Тестируем исправленный промпт
client = OllamaOCRClient(base_url="http://localhost:11434")

# Тестовый промпт
test_prompt = "<image>\nFree OCR."

# Протестируем на локальном файле
with open('test.jpg', 'rb') as f:
    image_data = f.read()

result = client.process_image(image_data)
print(f"Success: {result.get('success')}")
print(f"Response length: {len(result.get('response', ''))}")
print(f"First 500 chars: {result.get('response', '')[:500]}")