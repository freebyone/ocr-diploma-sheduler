import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

from minio_client import MinIOClient
from ollama_client import OllamaOCRClient

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/ocr_processor.log')
    ]
)

logger = logging.getLogger(__name__)

class OCRProcessor:
    def __init__(self):
        # Конфигурация из переменных окружения
        self.minio_config = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'ocrminio'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'admin123456'),
            'bucket_name': os.getenv('MINIO_BUCKET', 'document'),
            'secure': os.getenv('MINIO_SECURE', 'False').lower() == 'true'
        }
        
        self.ollama_config = {
            'base_url': os.getenv('OLLAMA_URL', 'http://localhost:11434'),
            'model': os.getenv('OLLAMA_MODEL', 'deepseek-ocr')
        }
        
        # Настройки обработки
        self.prompt_type = os.getenv('PROMPT_TYPE', 'markdown')  # По умолчанию markdown
        self.process_limit = int(os.getenv('PROCESS_LIMIT', '2'))  # Изображений за раз
        self.move_to_processed = os.getenv('MOVE_PROCESSED', 'True').lower() == 'true'
        self.process_interval = int(os.getenv('PROCESS_INTERVAL', '60'))  # Секунд между проверками
        
        # Инициализация клиентов
        self.minio_client = MinIOClient(**self.minio_config)
        self.ocr_client = OllamaOCRClient(**self.ollama_config)
        
        # Создаем директории для результатов
        os.makedirs('/app/results', exist_ok=True)
        os.makedirs('/app/logs', exist_ok=True)
        
        # Файл для отслеживания обработанных папок
        self.processed_file = '/app/processed_folders.txt'
    
    def load_processed_folders(self) -> set:
        """Загрузить список уже обработанных папок"""
        processed = set()
        if os.path.exists(self.processed_file):
            try:
                with open(self.processed_file, 'r') as f:
                    for line in f:
                        folder = line.strip()
                        if folder:
                            processed.add(folder)
                logger.info(f"Загружено {len(processed)} обработанных папок из файла")
            except Exception as e:
                logger.error(f"Ошибка при чтении файла обработанных папок: {e}")
        return processed
    
    def save_processed_folder(self, folder_name: str):
        """Сохранить информацию об обработанной папке"""
        try:
            with open(self.processed_file, 'a') as f:
                f.write(f"{folder_name}\n")
            logger.debug(f"Папка {folder_name} добавлена в список обработанных")
        except Exception as e:
            logger.error(f"Ошибка при сохранении обработанной папки: {e}")
    
    async def process_single_image(self, image_info: Dict[str, Any]) -> Dict[str, Any]:
        """Обработать одно изображение"""
        image_name = image_info['name']
        
        try:
            logger.info(f"Обработка: {image_name}")
            
            # Скачиваем изображение
            image_data = self.minio_client.download_image(image_name)
            if not image_data:
                error_msg = f"Не удалось загрузить изображение"
                logger.error(error_msg)
                return {
                    'image_name': image_name,
                    'success': False,
                    'error': error_msg
                }
            
            # Отправляем в OCR
            logger.info(f"  Отправка в OCR...")
            ocr_result = self.ocr_client.process_image(image_data, self.prompt_type)
            
            # Добавляем информацию об изображении
            ocr_result['image_name'] = image_name
            ocr_result['image_size'] = image_info['size']
            ocr_result['processed_at'] = datetime.now().isoformat()
            
            # Логируем результат
            if ocr_result.get('success', False):
                response_text = ocr_result.get('response', '')
                response_length = len(response_text)
                
                logger.info(f"  ✓ Успешно!")
                logger.info(f"    Время: {ocr_result.get('metrics', {}).get('total_duration_sec', 0):.2f}с")
                logger.info(f"    Токенов: {ocr_result.get('metrics', {}).get('eval_count', 0)}")
                logger.info(f"    Символов: {response_length}")
                
                # Показываем начало текста
                if response_text:
                    preview = response_text[:200]
                    if response_length > 200:
                        preview += "..."
                    logger.info(f"    Текст: {preview}")
            
            return ocr_result
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {str(e)}"
            logger.error(error_msg)
            return {
                'image_name': image_name,
                'success': False,
                'error': error_msg,
                'processed_at': datetime.now().isoformat()
            }
    
    async def process_folder(self, folder_name: str) -> bool:
        """Обработать все изображения в папке"""
        logger.info(f"═══════════════════════════════════════════════")
        logger.info(f"ОБРАБОТКА ПАПКИ: {folder_name}")
        logger.info(f"═══════════════════════════════════════════════")
        
        # Получаем список изображений
        images = self.minio_client.list_images_in_folder(folder_name)
        
        if not images:
            logger.warning(f"В папке {folder_name} не найдено изображений")
            return False
        
        logger.info(f"Найдено {len(images)} изображений")
        
        # Ограничиваем количество обрабатываемых изображений
        images_to_process = images[:self.process_limit]
        
        # Обрабатываем изображения последовательно
        results = []
        for i, image_info in enumerate(images_to_process, 1):
            logger.info(f"[{i}/{len(images_to_process)}]")
            result = await self.process_single_image(image_info)
            results.append(result)
            
            # Небольшая пауза между изображениями
            if i < len(images_to_process):
                await asyncio.sleep(1)
        
        # Сохраняем результаты
        success = any(r.get('success', False) for r in results)
        if results:
            self.save_results(folder_name, results)
            
            # Перемещаем папку в processed если нужно
            if self.move_to_processed and success:
                logger.info(f"Перемещение папки {folder_name} в processed...")
                moved = self.minio_client.move_folder_to_processed(folder_name)
                if moved:
                    logger.info(f"✓ Папка успешно перемещена")
                else:
                    logger.warning(f"⚠ Не удалось переместить папку")
            
            # Сохраняем информацию об обработанной папке
            self.save_processed_folder(folder_name)
        
        logger.info(f"═══════════════════════════════════════════════")
        logger.info(f"ЗАВЕРШЕНО: {folder_name}")
        logger.info(f"═══════════════════════════════════════════════\n")
        
        return success
    
    def save_results(self, folder_name: str, results: List[Dict[str, Any]]):
        """Сохранить результаты в файл"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # JSON для программы
        json_filename = f"/app/results/ocr_{folder_name}_{timestamp}.json"
        
        # Markdown для человека (если был запрос markdown)
        md_filename = f"/app/results/ocr_{folder_name}_{timestamp}.md"
        
        summary = {
            'folder_name': folder_name,
            'processed_at': datetime.now().isoformat(),
            'prompt_type': self.prompt_type,
            'total_images': len(results),
            'successful': sum(1 for r in results if r.get('success', False)),
            'failed': sum(1 for r in results if not r.get('success', False)),
            'results': results
        }
        
        # Сохраняем JSON
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        # Сохраняем Markdown (только если был запрос markdown и есть успешные результаты)
        successful_results = [r for r in results if r.get('success', False) and r.get('response')]
        if successful_results and self.prompt_type == 'markdown':
            with open(md_filename, 'w', encoding='utf-8') as f:
                f.write(f"# OCR Results: {folder_name}\n\n")
                f.write(f"**Processed:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"**Prompt Type:** {self.prompt_type}\n\n")
                
                for i, result in enumerate(successful_results, 1):
                    image_name = result.get('image_name', '').split('/')[-1]
                    f.write(f"## Image {i}: {image_name}\n\n")
                    f.write(f"**Size:** {result.get('image_size', 0)} bytes\n")
                    f.write(f"**Processing Time:** {result.get('metrics', {}).get('total_duration_sec', 0):.2f}s\n")
                    f.write(f"**Tokens:** {result.get('metrics', {}).get('eval_count', 0)}\n\n")
                    f.write("### Extracted Text:\n\n")
                    f.write(result.get('response', '') + "\n\n")
                    f.write("---\n\n")
        
        logger.info(f"Результаты сохранены:")
        logger.info(f"  JSON: {json_filename}")
        if successful_results and self.prompt_type == 'markdown':
            logger.info(f"  Markdown: {md_filename}")
        
        # Выводим сводку в консоль
        self.print_summary(summary, json_filename, md_filename if successful_results and self.prompt_type == 'markdown' else None)
    
    def print_summary(self, summary: Dict[str, Any], json_path: str, md_path: str = None):
        """Вывести сводку в консоль"""
        print("\n" + "="*80)
        print("📄 ОТЧЕТ ОБ ОБРАБОТКЕ OCR")
        print("="*80)
        print(f"📁 Папка: {summary['folder_name']}")
        print(f"🕒 Время: {summary['processed_at']}")
        print(f"📝 Тип промпта: {summary['prompt_type']}")
        print(f"🖼️  Всего изображений: {summary['total_images']}")
        print(f"✅ Успешно: {summary['successful']}")
        print(f"❌ Неудачно: {summary['failed']}")
        print("-"*80)
        
        # Показываем краткую информацию по каждому изображению
        for i, result in enumerate(summary['results'], 1):
            status = "✅" if result.get('success') else "❌"
            image_short = result.get('image_name', '').split('/')[-1]
            print(f"{i}. {image_short} {status}")
            
            if result.get('success'):
                response = result.get('response', '')
                duration = result.get('metrics', {}).get('total_duration_sec', 0)
                tokens = result.get('metrics', {}).get('eval_count', 0)
                print(f"   ⏱️  {duration:.2f}s | 🪙 {tokens} токенов | 📏 {len(response)} символов")
        
        print("="*80)
        print(f"📊 JSON результаты: {json_path}")
        if md_path:
            print(f"📝 Markdown результаты: {md_path}")
        print("="*80 + "\n")
    
    async def run_once(self):
        """Однократный запуск обработки"""
        logger.info("🚀 Запуск OCR процессора...")
        
        # Проверяем подключения
        if not self.minio_client.check_connection():
            logger.error("Не удалось подключиться к MinIO")
            return False
        
        if not self.ocr_client.check_health():
            logger.error("Не удалось подключиться к Ollama")
            return False
        
        logger.info("✓ Все подключения установлены\n")
        
        # Загружаем уже обработанные папки
        processed_folders = self.load_processed_folders()
        
        # Получаем необработанные папки
        all_folders = self.minio_client.get_unprocessed_folders()
        
        # Фильтруем уже обработанные
        new_folders = [f for f in all_folders if f not in processed_folders]
        
        if not new_folders:
            logger.info("🤷 Нет новых папок для обработки")
            return False
        
        logger.info(f"📂 Найдено {len(new_folders)} новых папок для обработки")
        
        # Обрабатываем первую новую папку
        folder_to_process = new_folders[0]
        logger.info(f"🎯 Выбрана папка для обработки: {folder_to_process}")
        
        # Обрабатываем папку
        success = await self.process_folder(folder_to_process)
        
        return success
    
    async def run_continuous(self):
        """Непрерывный цикл обработки"""
        logger.info("🔄 Запуск непрерывного режима обработки...")
        
        while True:
            try:
                await self.run_once()
                logger.info(f"⏳ Ожидание {self.process_interval} секунд до следующей проверки...\n")
                await asyncio.sleep(self.process_interval)
                
            except KeyboardInterrupt:
                logger.info("\n👋 Обработка прервана пользователем")
                break
            except Exception as e:
                logger.error(f"💥 Критическая ошибка: {str(e)}", exc_info=True)
                logger.info(f"⏳ Повторная попытка через {self.process_interval} секунд...\n")
                await asyncio.sleep(self.process_interval)

async def main():
    """Основная функция"""
    processor = OCRProcessor()
    
    try:
        # Режим работы: однократно или непрерывно
        mode = os.getenv('PROCESS_MODE', 'once').lower()
        
        if mode == 'continuous':
            await processor.run_continuous()
        else:
            await processor.run_once()
            
    except KeyboardInterrupt:
        logger.info("\n👋 Программа завершена")
    except Exception as e:
        logger.error(f"💥 Фатальная ошибка: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())