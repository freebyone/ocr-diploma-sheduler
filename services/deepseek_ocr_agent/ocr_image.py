import requests
import base64
import json
import sys

def encode_image_to_base64(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except FileNotFoundError:
        print(f"Ошибка: Файл {image_path} не найден")
        return None

def send_ocr_request(image_path, prompt_type="text"):
    """
    prompt_type:
    - "text": text
    - "markdown": markdown
    - "table": table
    """
    
    base64_image = encode_image_to_base64(image_path)
    if base64_image is None:
        return
    
    prompts = {
        "text": "<image>\nFree OCR.",
        "markdown": "<|grounding|>Convert the document to markdown.",  # markdown
        "table": "<|grounding|>Extract all tables from this image.",  # table
        "details": "<|grounding|>Provide detailed description of this image including text and structure."  # Подробно
    }
    
    prompt = prompts.get(prompt_type, prompts["text"])
    
    # query
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": "deepseek-ocr",
        "prompt": prompt,
        "images": [base64_image],  # Ключевой момент - массив изображений
        "stream": False,
        "options": {
            "temperature": 0.1,  # Низкая температура для более точного OCR
            "num_predict": 4096  # Максимальное количество токенов в ответе
        }
    }
    
    try:
        print(f"Отправляем запрос для {image_path}...")
        response = requests.post(url, json=payload, timeout=600)
        
        if response.status_code == 200:
            result = response.json()
            print("\n" + "="*50)
            print(f"УСПЕХ! Модель: {result.get('model', 'N/A')}")
            print(f"Продолжительность: {result.get('total_duration', 0)/1e9:.2f} сек")
            print("="*50)
            print("\nРАСПОЗНАННЫЙ ТЕКСТ:")
            print("-" * 50)
            print(result.get('response', 'Нет ответа'))
            print("-" * 50)
            
            print(f"\nДетали:")
            print(f"  Всего токенов: {result.get('eval_count', 0)}")
            print(f"  Причина завершения: {result.get('done_reason', 'N/A')}")
            
        else:
            print(f"Ошибка HTTP: {response.status_code}")
            print(response.text)
            
    except requests.exceptions.ConnectionError:
        print("Ошибка подключения. Убедитесь, что контейнер Ollama запущен:")
        print("  docker ps | grep ocr-container")
    except Exception as e:
        print(f"Ошибка: {e}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Тестирование DeepSeek-OCR')
    parser.add_argument('image_path', help='Путь к изображению')
    parser.add_argument('--prompt', '-p', choices=['text', 'markdown', 'table', 'details'],
                       default='text', help='Тип запроса (по умолчанию: text)')
    
    args = parser.parse_args()
    
    if len(sys.argv) < 2:
        print("Использование: python test_ocr_image.py <путь_к_изображению> [--prompt text|markdown|table|details]")
        print("\nПримеры:")
        print("  python test_ocr_image.py screenshot.png")
        print("  python test_ocr_image.py document.jpg --prompt markdown")
        print("  python test_ocr_image.py table.png --prompt table")
        return
    
    send_ocr_request(args.image_path, args.prompt)

if __name__ == "__main__":
    main()