import requests
import base64
import time
import os
import tempfile
import logging
import traceback
import sys
from minio import Minio
from minio.error import S3Error
import json
from typing import List, Dict, Any, Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class OCRProcessor:
    def __init__(self):
        # Проверка версии MinIO
        try:
            from importlib.metadata import version
            minio_version = version("minio")
            logger.info(f"MinIO library version: {minio_version}")
        except Exception:
            try:
                import pkg_resources
                minio_version = pkg_resources.get_distribution("minio").version
                logger.info(f"MinIO library version: {minio_version}")
            except Exception:
                logger.warning("Could not determine MinIO version")

        self.minio_client = Minio(
            "minio:9000",
            access_key="ocrminio",
            secret_key="admin123456",
            secure=False
        )

        self.ollama_url = "http://ollama:11434/api/generate"
        self.model = "deepseek-ocr"

        self.source_bucket = "documents"
        self.results_bucket = "results"
        self.errors_bucket = "errors"

        self._ensure_buckets()

    def _ensure_buckets(self):
        """Создаём бакеты если не существуют."""
        for bucket in [self.source_bucket, self.results_bucket, self.errors_bucket]:
            try:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except Exception as e:
                logger.error(f"Error ensuring bucket {bucket}: {e}")

    # ──────────────────────────────────────────────
    # Работа с файлами в бакете (без папок)
    # ──────────────────────────────────────────────

    def list_images(self) -> List[str]:
        """
        Получает список всех .jpg файлов в source_bucket.
        Возвращает список имён вида ['0001.jpg', '0102.jpg', ...]
        """
        images = []
        try:
            objects = self.minio_client.list_objects(
                self.source_bucket,
                recursive=True
            )
            for obj in objects:
                name = obj.object_name
                if name.lower().endswith('.jpg') and '/' not in name:
                    images.append(name)
        except Exception as e:
            logger.error(f"Error listing images: {e}")
        return images

    def get_prefix_from_image(self, image_name: str) -> str:
        """
        Извлекает префикс из имени файла.
        '0001.jpg' → '0001'
        """
        return os.path.splitext(image_name)[0]

    def download_image(self, image_name: str) -> Optional[str]:
        """Скачивает изображение из MinIO во временный файл."""
        try:
            temp_file = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            temp_path = temp_file.name
            temp_file.close()

            self.minio_client.fget_object(self.source_bucket, image_name, temp_path)
            return temp_path
        except Exception as e:
            logger.error(f"Error downloading {image_name}: {e}")
            return None

    def delete_image_from_source(self, image_name: str) -> bool:
        """Удаляет одно изображение из source_bucket."""
        try:
            self.minio_client.remove_object(self.source_bucket, image_name)
            logger.info(f"Deleted from source: {image_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting {image_name} from source: {e}")
            return False

    # ──────────────────────────────────────────────
    # OCR обработка одного изображения через Ollama
    # ──────────────────────────────────────────────

    def ocr_image(self, image_name: str) -> Dict[str, Any]:
        """
        Скачивает изображение, отправляет в Ollama OCR,
        возвращает результат.
        """
        local_path = self.download_image(image_name)
        if not local_path:
            return {
                "image": image_name,
                "error": "Failed to download image",
                "status": "error",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

        try:
            with open(local_path, "rb") as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')

            prompt = "Describe this image in detail."

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

            logger.info(f"Sending to Ollama: {image_name}")
            start_time = time.time()

            response = requests.post(
                self.ollama_url,
                json=payload,
                timeout=600,
                headers={"Content-Type": "application/json"}
            )

            elapsed = time.time() - start_time

            if response.status_code == 200:
                result = response.json()

                done_reason = result.get('done_reason', 'unknown')
                ocr_text = result.get('response', '').strip()

                status = "success" if done_reason == 'stop' else "partial"

                logger.info(
                    f"{'Success' if status == 'success' else 'Partial'} in {elapsed:.1f}s "
                    f"(reason: {done_reason}), text length: {len(ocr_text)}"
                )

                return {
                    "image": image_name,
                    "ocr_text": ocr_text,
                    "processing_time": elapsed,
                    "model": self.model,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "done_reason": done_reason,
                    "eval_count": result.get('eval_count', 0),
                    "total_duration": result.get('total_duration', 0),
                    "status": status
                }
            else:
                logger.error(f"Ollama HTTP error: {response.status_code}")
                return {
                    "image": image_name,
                    "error": f"HTTP {response.status_code}",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "error"
                }

        except requests.exceptions.Timeout:
            logger.error(f"Timeout processing {image_name} after 600s")
            return {
                "image": image_name,
                "error": "Timeout after 600 seconds",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "error"
            }
        except Exception as e:
            logger.error(f"Error processing {image_name}: {e}")
            logger.error(traceback.format_exc())
            return {
                "image": image_name,
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "error"
            }
        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

    # ──────────────────────────────────────────────
    # Сохранение результатов
    # ──────────────────────────────────────────────

    def save_json_to_bucket(self, bucket_name: str, object_name: str, data: Dict) -> bool:
        """
        Сохраняет JSON в указанный бакет.
        object_name — имя файла, например '0001.json'
        """
        temp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.json', encoding='utf-8', delete=False
            ) as temp_file:
                temp_file_path = temp_file.name
                json.dump(data, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())

            file_size = os.path.getsize(temp_file_path)
            logger.info(f"JSON file size: {file_size / 1024:.2f} KB")

            with open(temp_file_path, 'rb') as file_data:
                self.minio_client.put_object(
                    bucket_name,
                    object_name,
                    file_data,
                    file_size,
                    content_type="application/json; charset=utf-8"
                )

            logger.info(f"Saved to {bucket_name}/{object_name}")

            # Верификация
            self._verify_saved_file(bucket_name, object_name, file_size)

            return True

        except Exception as e:
            logger.error(f"Error saving to {bucket_name}/{object_name}: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception:
                    pass

    def _verify_saved_file(self, bucket: str, filename: str, expected_size: int) -> bool:
        """Проверяет что файл загрузился корректно."""
        try:
            obj_info = self.minio_client.stat_object(bucket, filename)
            actual_size = obj_info.size

            if actual_size == expected_size:
                logger.info(f"File verification passed: {filename} ({actual_size} bytes)")
                return True
            else:
                logger.error(
                    f"File size mismatch for {filename}: "
                    f"expected {expected_size}, got {actual_size}"
                )
                return False
        except Exception as e:
            logger.error(f"Error verifying file {filename}: {e}")
            return False

    # ──────────────────────────────────────────────
    # Обработка одного изображения (полный цикл)
    # ──────────────────────────────────────────────

    def process_image(self, image_name: str) -> bool:
        """
        Полный цикл обработки одного изображения:
        1. OCR через Ollama
        2. Если done_reason == 'stop' → сохраняем в results/{prefix}.json
        3. Иначе → сохраняем в errors/{prefix}.json
        4. Удаляем исходный файл из documents
        
        Пример:
            documents/0001.jpg
            → OCR → results/0001.json (или errors/0001.json)
            → удаляем documents/0001.jpg
        """
        prefix = self.get_prefix_from_image(image_name)
        json_name = f"{prefix}.json"

        logger.info(f"=== Processing image: {image_name} (prefix: {prefix}) ===")

        # 1. OCR
        result = self.ocr_image(image_name)

        # 2. Обёртка для сохранения
        result_data = {
            "prefix": prefix,
            "source_image": image_name,
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "result": result
        }

        status = result.get("status", "error")
        done_reason = result.get("done_reason", "unknown")

        # 3. Определяем куда сохранять
        if status == "success" and done_reason == "stop":
            # Всё хорошо → results
            target_bucket = self.results_bucket
            logger.info(f"✅ OCR successful (reason: stop) → saving to {target_bucket}/{json_name}")
        else:
            # Проблема → errors
            target_bucket = self.errors_bucket
            reason = result.get("error", f"done_reason={done_reason}, status={status}")
            result_data["error_reason"] = reason
            logger.warning(
                f"⚠️ OCR issue for {image_name}: {reason} → saving to {target_bucket}/{json_name}"
            )

        # 4. Сохраняем JSON
        save_success = self.save_json_to_bucket(target_bucket, json_name, result_data)

        if save_success:
            # 5. Удаляем исходный файл из documents
            logger.info(f"Deleting source image: {image_name}")
            self.delete_image_from_source(image_name)
        else:
            logger.error(f"Failed to save result for {image_name}, keeping source file")

        return status == "success" and done_reason == "stop"

    # ──────────────────────────────────────────────
    # Главный цикл
    # ──────────────────────────────────────────────

    def run(self):
        logger.info("=== Starting OCR Processor ===")

        # Проверка подключения к MinIO
        try:
            buckets = self.minio_client.list_buckets()
            logger.info(
                f"Successfully connected to MinIO. "
                f"Available buckets: {[b.name for b in buckets]}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            logger.error("Check if MinIO is running and credentials are correct")
            return

        logger.info(f"Source bucket: {self.source_bucket}")
        logger.info(f"Results bucket: {self.results_bucket}")
        logger.info(f"Errors bucket: {self.errors_bucket}")
        logger.info("")
        logger.info("File format: documents/0001.jpg → results/0001.json or errors/0001.json")
        logger.info("")
        logger.info("Rules:")
        logger.info("  - done_reason: stop → results bucket (0001.json)")
        logger.info("  - done_reason: length / error → errors bucket (0001.json)")
        logger.info("  - Source image deleted after processing")
        logger.info("")
        logger.info("Waiting for images to process...")

        while True:
            try:
                images = self.list_images()

                if images:
                    logger.info(f"Found {len(images)} images to process: {images}")

                for image_name in images:
                    try:
                        self.process_image(image_name)
                    except Exception as e:
                        logger.error(f"Error processing {image_name}: {e}")
                        logger.error(traceback.format_exc())

                        # Пытаемся сохранить ошибку и удалить исходник
                        try:
                            prefix = self.get_prefix_from_image(image_name)
                            error_data = {
                                "prefix": prefix,
                                "source_image": image_name,
                                "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "error_reason": str(e),
                                "result": {
                                    "image": image_name,
                                    "error": str(e),
                                    "status": "error",
                                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                            }
                            self.save_json_to_bucket(
                                self.errors_bucket,
                                f"{prefix}.json",
                                error_data
                            )
                            self.delete_image_from_source(image_name)
                        except Exception as inner_e:
                            logger.error(f"Error in error handling for {image_name}: {inner_e}")

                    # Пауза между изображениями
                    time.sleep(1)

                if not images:
                    logger.debug("No images found. Checking again in 30 seconds...")

                time.sleep(30)

            except KeyboardInterrupt:
                logger.info("Stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.error(traceback.format_exc())
                time.sleep(30)


def main():
    processor = OCRProcessor()
    processor.run()


if __name__ == "__main__":
    main()