"""
Сервис парсинга OCR результатов из MinIO → PostgreSQL.

Новая структура (без папок):
1. Мониторит бакет 'results' — файлы вида 0001.json, 0102.json
2. Каждый JSON содержит result.ocr_text (одна страница)
3. Извлекает ФИО, направление, учебное заведение, специальность
4. Все поля найдены → записывает в БД, удаляет 0001.json из results
5. Что-то не найдено → перемещает 0001.json в errors
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
from db_functions import save_diploma_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

model = GigaChat(
    model=config.GIGACHAT_MODEL,
    credentials=config.GIGACHAT_CREDENTIALS,
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

    # ──────────────────────────────────────────────
    # Работа с файлами (без папок)
    # ──────────────────────────────────────────────

    def list_result_files(self) -> List[str]:
        """
        Список JSON файлов в results бакете.
        Возвращает: ['0001.json', '0102.json', ...]
        """
        files = []
        try:
            objects = self.minio_client.list_objects(
                self.results_bucket, recursive=True
            )
            for obj in objects:
                name = obj.object_name
                if name.lower().endswith('.json') and '/' not in name:
                    files.append(name)
        except Exception as e:
            logger.error(f"Error listing result files: {e}")
        return files

    def get_prefix_from_filename(self, filename: str) -> str:
        """
        '0001.json' → '0001'
        """
        return os.path.splitext(filename)[0]

    def download_json(self, filename: str) -> Optional[Dict]:
        """Скачивает и парсит JSON из results бакета"""
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                suffix='.json', delete=False
            ) as tf:
                temp_path = tf.name

            self.minio_client.fget_object(
                self.results_bucket, filename, temp_path
            )

            with open(temp_path, 'r', encoding='utf-8') as f:
                return json.load(f)

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return None
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    def get_ocr_text(self, data: Dict) -> Optional[str]:
        """
        Извлекает OCR текст из нового формата JSON.
        
        Формат:
        {
            "prefix": "0001",
            "source_image": "0001.jpg",
            "result": {
                "ocr_text": "...",
                ...
            }
        }
        """
        result = data.get("result")
        if not result:
            logger.error("No 'result' field in JSON data")
            return None

        ocr_text = result.get("ocr_text", "").strip()
        if not ocr_text:
            logger.error("Empty OCR text in result")
            return None

        return ocr_text

    # ──────────────────────────────────────────────
    # Сохранение и перемещение
    # ──────────────────────────────────────────────

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

    def delete_from_results(self, filename: str):
        """Удаляет один файл из results бакета"""
        try:
            self.minio_client.remove_object(self.results_bucket, filename)
            logger.info(f"Deleted from results: {filename}")
        except Exception as e:
            logger.error(f"Error deleting {filename} from results: {e}")

    def move_to_errors(
        self,
        filename: str,
        data: Dict,
        parsed: ParsedStudent,
        reason: str
    ):
        """
        Перемещает файл в errors бакет с информацией об ошибке.
        filename: '0001.json'
        """
        try:
            prefix = self.get_prefix_from_filename(filename)

            error_data = {
                "original_file": filename,
                "prefix": prefix,
                "reason": reason,
                "requires_manual_review": True,
                "parsed_data": {
                    "full_name": parsed.full_name,
                    "direction": parsed.direction,
                    "university": parsed.university,
                    "specialization": parsed.specialization,
                    "code": parsed.code,
                },
                "missing_fields": parsed.missing_fields,
                "parse_errors": parsed.errors,
                "moved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "original_data": data
            }

            # Сохраняем ошибку как {prefix}.json в errors
            error_filename = f"{prefix}.json"
            self._save_json_to_minio(
                self.errors_bucket, error_filename, error_data
            )

            # Удаляем из results
            self.delete_from_results(filename)

        except Exception as e:
            logger.error(f"Error moving {filename} to errors: {e}")
            logger.error(traceback.format_exc())

    # ──────────────────────────────────────────────
    # Обработка одного файла
    # ──────────────────────────────────────────────

    def process_file(self, filename: str) -> bool:
        """
        Обрабатывает один JSON файл из results.
        
        filename: '0001.json'
        
        Возвращает True если успешно записано в БД.
        """
        prefix = self.get_prefix_from_filename(filename)

        logger.info("=" * 60)
        logger.info(f"Processing: {filename} (prefix: {prefix})")

        # 1. Скачиваем JSON
        data = self.download_json(filename)
        if not data:
            logger.error(f"Failed to download/parse: {filename}")
            return False

        source_image = data.get("source_image", f"{prefix}.jpg")
        logger.info(f"Source image: {source_image}")

        # 2. Извлекаем OCR текст
        ocr_text = self.get_ocr_text(data)
        if not ocr_text:
            empty_parsed = ParsedStudent()
            empty_parsed.errors.append("Не найден OCR текст")
            self.move_to_errors(
                filename, data, empty_parsed,
                "Не найден OCR текст в результате"
            )
            return False

        logger.info(f"OCR text length: {len(ocr_text)} chars")
        logger.info(f"OCR text preview: {ocr_text[:200]}...")

        # 3. Парсим через LLM
        parser = LLMParser(model)
        parsed = parser.parse_image_text(ocr_text)

        if parsed is None:
            empty_parsed = ParsedStudent()
            empty_parsed.errors.append("LLM вернул None")
            self.move_to_errors(
                filename, data, empty_parsed,
                "LLM не смог распарсить текст"
            )
            return False

        logger.info(f"Parsed data:")
        logger.info(f"  FIO:            {parsed.full_name}")
        logger.info(f"  Direction:      {parsed.direction}")
        logger.info(f"  University:     {parsed.university}")
        logger.info(f"  Specialization: {parsed.specialization}")
        logger.info(f"  Code:           {parsed.code}")

        # 4. Проверяем валидность
        if not parsed.is_valid:
            logger.warning(
                f"Parsing incomplete for {filename}. "
                f"Missing: {parsed.missing_fields}"
            )
            self.move_to_errors(
                filename, data, parsed,
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
                specialization_name=parsed.specialization,
                specialization_code=parsed.code or "",
                file_code=prefix,
                file_name=source_image
            )
            session.commit()

            logger.info(
                f"✅ Saved to DB: "
                f"student_id={db_result['student'].id}, "
                f"spec_id={db_result['specialization'].id}, "
                f"file_code={prefix}"
            )

            # 6. Удаляем из results
            self.delete_from_results(filename)
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            logger.error(traceback.format_exc())

            parsed.errors.append(f"Ошибка записи в БД: {str(e)}")
            self.move_to_errors(
                filename, data, parsed,
                f"Ошибка записи в БД: {str(e)}"
            )
            return False
        finally:
            session.close()

    # ──────────────────────────────────────────────
    # Главный цикл
    # ──────────────────────────────────────────────

    def run(self):
        """Основной цикл"""
        logger.info("=" * 60)
        logger.info("Starting Diploma Parser Service")
        logger.info(f"Results bucket: {self.results_bucket}")
        logger.info(f"Errors bucket:  {self.errors_bucket}")
        logger.info(f"Database:       {config.DATABASE_URL}")
        logger.info(f"Poll interval:  {config.POLL_INTERVAL}s")
        logger.info("")
        logger.info("File format: results/0001.json → DB + delete")
        logger.info("")

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
        logger.info("  - Read 0001.json from 'results' bucket")
        logger.info("  - Extract OCR text from result.ocr_text")
        logger.info("  - Parse: FIO, direction, university, specialization")
        logger.info("  - All fields found → save to DB, delete from results")
        logger.info("  - Any field missing → move to errors (manual review)")

        while True:
            try:
                files = self.list_result_files()

                if files:
                    logger.info(f"Found {len(files)} result files: {files}")

                for filename in files:
                    try:
                        self.process_file(filename)
                    except Exception as e:
                        logger.error(f"Error processing {filename}: {e}")
                        logger.error(traceback.format_exc())

                        # Аварийная обработка
                        try:
                            prefix = self.get_prefix_from_filename(filename)
                            error_parsed = ParsedStudent()
                            error_parsed.errors.append(str(e))
                            self.move_to_errors(
                                filename,
                                {"error": str(e)},
                                error_parsed,
                                f"Необработанная ошибка: {str(e)}"
                            )
                        except Exception as inner_e:
                            logger.error(
                                f"Error in error handling for {filename}: "
                                f"{inner_e}"
                            )

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