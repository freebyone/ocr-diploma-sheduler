"""
Сервис парсинга OCR результатов из MinIO → PostgreSQL.

1. Мониторит бакет 'results'
2. Берёт JSON, парсит первую страницу (фото 1)
3. Извлекает ФИО, направление, учебное заведение, специальность
4. Все 4 поля найдены → записывает в БД, удаляет из results
5. Что-то не найдено → перемещает в errors (требуется ручной разбор)
"""

import json
import time
import os
import sys
import tempfile
import logging
import traceback
from typing import List, Dict, Optional

# ==LLM==
from langchain_gigachat.chat_models import GigaChat
from ocr_llm_parser import LLMParser, ParsedStudent
# ==LLM==

from minio import Minio

import config
from database import init_db, get_session, wait_for_db
from ocr_parser import parse_first_page, ParsedDiploma
from db_functions import save_diploma_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

model = GigaChat(
        model="GigaChat-2",
        credentials=config.MODEL_KEY,
        verify_ssl_certs=False,
        top_p=0,
        temperature=0.1
    )

class DiplomaParserService:

    def __init__(self):
        self.minio_client = Minio(
            config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=config.MINIO_SECURE
        )
        self.results_bucket = config.RESULTS_BUCKET
        self.errors_bucket = config.ERRORS_BUCKET
        self._ensure_buckets()

    def _ensure_buckets(self):
        for bucket in [self.results_bucket, self.errors_bucket]:
            try:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except Exception as e:
                logger.error(f"Error ensuring bucket {bucket}: {e}")

    def list_result_files(self) -> List[str]:
        """Список JSON файлов в results бакете"""
        files = []
        try:
            objects = self.minio_client.list_objects(
                self.results_bucket, recursive=True
            )
            for obj in objects:
                if obj.object_name.endswith('.json'):
                    files.append(obj.object_name)
        except Exception as e:
            logger.error(f"Error listing result files: {e}")
        return files

    def download_json(self, file_path: str) -> Optional[Dict]:
        """Скачивает и парсит JSON из MinIO"""
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                suffix='.json', delete=False
            ) as tf:
                temp_path = tf.name

            self.minio_client.fget_object(
                self.results_bucket, file_path, temp_path
            )

            with open(temp_path, 'r', encoding='utf-8') as f:
                return json.load(f)

        except Exception as e:
            logger.error(f"Error downloading {file_path}: {e}")
            return None
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    def get_first_page_text(self, data: Dict) -> Optional[str]:
        """
        Извлекает OCR текст первой страницы.
        Ищет файл с именем "1" (1.jpg, 1.png и т.д.),
        если не найден — берёт первый по сортировке.
        """
        results = data.get("results", [])
        if not results:
            logger.error("No results in JSON data")
            return None

        first_page = None
        for result in results:
            image_path = result.get("image_path", "")
            filename = image_path.split('/')[-1] if '/' in image_path else image_path
            name_without_ext = os.path.splitext(filename)[0]

            if name_without_ext == "1":
                first_page = result
                break

        if not first_page:
            sorted_results = sorted(
                results, key=lambda r: r.get("image_path", "")
            )
            first_page = sorted_results[0]
            logger.warning(
                f"Page '1' not found, using: {first_page.get('image_path')}"
            )

        ocr_text = first_page.get("ocr_text", "")
        if not ocr_text:
            logger.error(
                f"Empty OCR text for: {first_page.get('image_path')}"
            )
            return None

        return ocr_text

    def _save_json_to_minio(
        self, bucket: str, object_name: str, data: Dict
    ):
        """Сохраняет JSON в MinIO"""
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.json',
                encoding='utf-8', delete=False
            ) as tf:
                temp_path = tf.name
                json.dump(data, tf, indent=2, ensure_ascii=False)
                tf.flush()
                os.fsync(tf.fileno())

            file_size = os.path.getsize(temp_path)

            with open(temp_path, 'rb') as f:
                self.minio_client.put_object(
                    bucket, object_name, f, file_size,
                    content_type="application/json; charset=utf-8"
                )

            logger.info(f"Saved to {bucket}/{object_name} ({file_size} bytes)")

        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    def move_to_errors(
        self,
        file_path: str,
        data: Dict,
        parsed: ParsedDiploma,
        reason: str
    ):
        """Перемещает файл в errors бакет с информацией об ошибке"""
        try:
            folder = file_path.split('/')[0] if '/' in file_path else "unknown"

            error_data = {
                "original_file": file_path,
                "folder": folder,
                "reason": reason,
                "requires_manual_review": True,
                "parsed_data": {
                    "full_name": parsed.full_name,
                    "direction": parsed.direction,
                    "university": parsed.university,
                    "specialization": parsed.specialization,
                },
                "missing_fields": parsed.missing_fields,
                "parse_errors": parsed.errors,
                "moved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "original_data": data
            }

            error_filename = f"{folder}/parse_error.json"
            self._save_json_to_minio(
                self.errors_bucket, error_filename, error_data
            )

            # Копируем оригинальный JSON
            try:
                from minio.commonconfig import CopySource
                self.minio_client.copy_object(
                    self.errors_bucket,
                    file_path,
                    CopySource(self.results_bucket, file_path)
                )
            except ImportError:
                try:
                    self.minio_client.copy_object(
                        self.errors_bucket,
                        file_path,
                        f"{self.results_bucket}/{file_path}"
                    )
                except Exception as e:
                    logger.error(f"Error copying original file: {e}")

            # Удаляем из results
            self.delete_folder_from_results(folder)

        except Exception as e:
            logger.error(f"Error moving to errors: {e}")
            logger.error(traceback.format_exc())

    def delete_folder_from_results(self, folder: str):
        """Удаляет всю папку из results бакета"""
        try:
            prefix = f"{folder}/"
            objects = list(self.minio_client.list_objects(
                self.results_bucket, prefix=prefix, recursive=True
            ))

            for obj in objects:
                self.minio_client.remove_object(
                    self.results_bucket, obj.object_name
                )
                logger.debug(f"Deleted: {obj.object_name}")

            # Удаляем файлы без префикса папки
            # (на случай если файл лежит как folder/ocr_result.json)
            try:
                self.minio_client.remove_object(
                    self.results_bucket, f"{folder}"
                )
            except Exception:
                pass

            logger.info(f"Deleted folder from results: {folder}")
        except Exception as e:
            logger.error(f"Error deleting folder {folder}: {e}")

    def process_file(self, file_path: str) -> bool:
        """
        Обрабатывает один JSON файл.
        Возвращает True если успешно записано в БД.
        """
        logger.info("=" * 60)
        logger.info(f"Processing: {file_path}")

        # 1. Скачиваем JSON
        data = self.download_json(file_path)
        if not data:
            logger.error(f"Failed to download/parse: {file_path}")
            return False

        folder = data.get("folder", "unknown")
        logger.info(f"Folder: {folder}")

        # 2. Извлекаем текст первой страницы
        first_page_text = self.get_first_page_text(data)
        if not first_page_text:
            empty_parsed = ParsedDiploma()
            empty_parsed.errors.append("Не найден OCR текст первой страницы")
            self.move_to_errors(
                file_path, data, empty_parsed,
                "Не найден OCR текст первой страницы"
            )
            return False

        # 3. Парсим
        parser = LLMParser(model)
        parsed = parser.parse_image_text(first_page_text)
        parsed = parse_first_page(first_page_text)

        # 4. Проверяем
        if not parsed.is_valid:
            logger.warning(
                f"Parsing incomplete for {folder}. "
                f"Missing: {parsed.missing_fields}"
            )
            self.move_to_errors(
                file_path, data, parsed,
                f"Требуется ручной разбор. "
                f"Не распознаны: {', '.join(parsed.missing_fields)}"
            )
            return False

        # 5. Записываем в БД
        session = get_session()
        try:
            db_result = save_diploma_data(
                session=session,
                full_name=parsed.full_name,
                direction_name=parsed.direction,
                university_name=parsed.university,
                specialization_name=parsed.specialization
            )
            session.commit()

            logger.info(
                f"Saved to DB: "
                f"student_id={db_result['student'].id}, "
                f"spec_id={db_result['specialization'].id}"
            )

            # 6. Удаляем из results
            self.delete_folder_from_results(folder)
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            logger.error(traceback.format_exc())

            parsed.errors.append(f"Ошибка записи в БД: {str(e)}")
            self.move_to_errors(
                file_path, data, parsed,
                f"Ошибка записи в БД: {str(e)}"
            )
            return False

        finally:
            session.close()

    def run(self):
        """Основной цикл"""
        logger.info("=" * 60)
        logger.info("Starting Diploma Parser Service")
        logger.info(f"Results bucket: {self.results_bucket}")
        logger.info(f"Errors bucket:  {self.errors_bucket}")
        logger.info(f"Database:       {config.DATABASE_URL}")
        logger.info(f"Poll interval:  {config.POLL_INTERVAL}s")

        # Ждём БД
        if not wait_for_db():
            logger.error("Cannot connect to database. Exiting.")
            return

        # Инициализация таблиц
        try:
            init_db()
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            return

        # Проверка MinIO
        try:
            buckets = self.minio_client.list_buckets()
            logger.info(
                f"Connected to MinIO. Buckets: "
                f"{[b.name for b in buckets]}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            return

        logger.info("Waiting for results to process...")
        logger.info("Logic:")
        logger.info("  - Read JSON from 'results' bucket")
        logger.info("  - Parse first page (photo 1): "
                     "FIO, direction, university, specialization")
        logger.info("  - All 4 fields found -> save to DB, "
                     "delete from results")
        logger.info("  - Any field missing -> move to errors "
                     "(manual review)")

        while True:
            try:
                files = self.list_result_files()

                if files:
                    logger.info(f"Found {len(files)} result files")

                for file_path in files:
                    try:
                        self.process_file(file_path)
                    except Exception as e:
                        logger.error(f"Error processing {file_path}: {e}")
                        logger.error(traceback.format_exc())

                if not files:
                    logger.debug("No result files. Waiting...")

                time.sleep(config.POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.error(traceback.format_exc())
                time.sleep(config.POLL_INTERVAL)


def main():
    service = DiplomaParserService()
    service.run()


if __name__ == "__main__":
    main()