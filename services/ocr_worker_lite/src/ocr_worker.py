import requests
import base64
import time
import os
import tempfile
import logging
import traceback
import sys
from minio import Minio
from minio.commonconfig import CopySource
import json
from typing import List, Dict, Any, Optional

# --------------------------------------------------
# LOGGING CONFIG
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


# ==================================================
# OCR PROCESSOR
# ==================================================

class OCRProcessor:

    def __init__(self):

        self.minio_client = Minio(
            "minio:9000",
            access_key="ocrminio",
            secret_key="admin123456",
            secure=False
        )

        self.ollama_url = "http://ollama:11434/api/generate"
        self.model = "deepseek-ocr"

        self.source_bucket = "documents-lite"
        self.retry_bucket = "retry"
        self.results_bucket = "results"
        self.errors_bucket = "errors"

        self.sleep_interval = 30
        self.retry_delay_between_files = 1

        self._ensure_buckets()

    # ==================================================
    # BUCKET INIT
    # ==================================================

    def _ensure_buckets(self):
        for bucket in [
            self.source_bucket,
            self.retry_bucket,
            self.results_bucket,
            self.errors_bucket
        ]:
            try:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except Exception as e:
                logger.error(f"Bucket init error {bucket}: {e}")
                raise

    # ==================================================
    # LIST FILES
    # ==================================================

    def list_images(self, bucket_name: str) -> List[str]:
        images = []
        try:
            objects = self.minio_client.list_objects(bucket_name, recursive=False)
            for obj in objects:
                if obj.object_name.lower().endswith(".jpg"):
                    images.append(obj.object_name)
        except Exception as e:
            logger.error(f"List error in {bucket_name}: {e}")
        return images

    # ==================================================
    # FILE OPERATIONS
    # ==================================================

    def download_image(self, bucket: str, name: str) -> Optional[str]:
        try:
            temp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
            temp_path = temp.name
            temp.close()

            self.minio_client.fget_object(bucket, name, temp_path)
            return temp_path
        except Exception as e:
            logger.error(f"Download error {bucket}/{name}: {e}")
            return None

    def delete_object(self, bucket: str, name: str):
        try:
            self.minio_client.remove_object(bucket, name)
            logger.info(f"Deleted {bucket}/{name}")
        except Exception as e:
            logger.error(f"Delete error {bucket}/{name}: {e}")

    def move_object(self, source_bucket: str, target_bucket: str, name: str) -> bool:
        """
        Безопасное перемещение:
        1. Copy
        2. Проверка что объект появился
        3. Remove source
        """
        try:
            copy_source = CopySource(source_bucket, name)

            # Копируем
            self.minio_client.copy_object(
                target_bucket,
                name,
                copy_source
            )

            # Проверяем что файл реально существует в target
            self.minio_client.stat_object(target_bucket, name)

            # Удаляем из source
            self.minio_client.remove_object(source_bucket, name)

            logger.info(f"Moved {name} {source_bucket} → {target_bucket}")
            return True

        except Exception as e:
            logger.error(f"Move error {source_bucket}/{name}: {e}")
            logger.error(traceback.format_exc())
            return False

    # ==================================================
    # OCR CALL
    # ==================================================

    def ocr_image(self, bucket: str, name: str) -> Dict[str, Any]:

        local_path = self.download_image(bucket, name)
        if not local_path:
            return {"status": "error", "error": "download_failed"}

        try:
            with open(local_path, "rb") as f:
                image_b64 = base64.b64encode(f.read()).decode()

            payload = {
                "model": self.model,
                "prompt": "Convert the document to txt format.",
                "images": [image_b64],
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 4096
                }
            }

            start = time.time()

            response = requests.post(
                self.ollama_url,
                json=payload,
                timeout=600
            )

            elapsed = time.time() - start

            if response.status_code != 200:
                return {
                    "status": "error",
                    "error": f"http_{response.status_code}"
                }

            result = response.json()

            return {
                "status": "success" if result.get("done_reason") == "stop" else "partial",
                "ocr_text": result.get("response", "").strip(),
                "done_reason": result.get("done_reason"),
                "eval_count": result.get("eval_count"),
                "total_duration": result.get("total_duration"),
                "processing_time_sec": round(elapsed, 2)
            }

        except requests.exceptions.Timeout:
            return {"status": "error", "error": "timeout_600s"}
        except Exception as e:
            logger.error(traceback.format_exc())
            return {"status": "error", "error": str(e)}
        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

    # ==================================================
    # SAVE JSON WITH VERIFICATION
    # ==================================================

    def save_json(self, bucket: str, name: str, data: Dict) -> bool:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                delete=False
            ) as tmp:
                json.dump(data, tmp, indent=2, ensure_ascii=False)
                temp_path = tmp.name

            size = os.path.getsize(temp_path)

            with open(temp_path, "rb") as f:
                self.minio_client.put_object(
                    bucket,
                    name,
                    f,
                    size,
                    content_type="application/json"
                )

            # Проверяем размер
            obj = self.minio_client.stat_object(bucket, name)
            if obj.size != size:
                logger.error(f"Size mismatch for {bucket}/{name}")
                return False

            logger.info(f"Saved {bucket}/{name} ({size} bytes)")
            return True

        except Exception as e:
            logger.error(f"Save JSON error {bucket}/{name}: {e}")
            logger.error(traceback.format_exc())
            return False

        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    # ==================================================
    # PROCESS SINGLE FILE
    # ==================================================

    def process_image(self, bucket: str, name: str):

        prefix = os.path.splitext(name)[0]
        json_name = f"{prefix}.json"

        logger.info(f"Processing {bucket}/{name}")

        result = self.ocr_image(bucket, name)

        payload = {
            "source_bucket": bucket,
            "image": name,
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "model": self.model,
            "result": result
        }

        status = result.get("status")
        done_reason = result.get("done_reason")

        # ✅ SUCCESS
        if status == "success" and done_reason == "stop":
            if self.save_json(self.results_bucket, json_name, payload):
                self.delete_object(bucket, name)
            return

        # ⚠ FIRST FAILURE
        if bucket == self.source_bucket:
            logger.warning(f"First failure → retry/{name}")
            moved = self.move_object(self.source_bucket, self.retry_bucket, name)
            if not moved:
                logger.error(f"CRITICAL: Failed to move {name} to retry")
            return

        # ❌ SECOND FAILURE
        if bucket == self.retry_bucket:
            logger.error(f"Second failure → errors/{json_name}")
            if self.save_json(self.errors_bucket, json_name, payload):
                self.delete_object(self.retry_bucket, name)
            return

    # ==================================================
    # MAIN LOOP
    # ==================================================

    def run(self):

        logger.info("=== OCR Processor STARTED (PRODUCTION MODE) ===")

        while True:
            try:
                docs = self.list_images(self.source_bucket)

                if docs:
                    logger.info(f"{len(docs)} files in documents-lite")
                    for name in docs:
                        self.process_image(self.source_bucket, name)
                        time.sleep(self.retry_delay_between_files)
                else:
                    retry_files = self.list_images(self.retry_bucket)

                    if retry_files:
                        logger.info(f"{len(retry_files)} files in retry")
                        for name in retry_files:
                            self.process_image(self.retry_bucket, name)
                            time.sleep(self.retry_delay_between_files)

                time.sleep(self.sleep_interval)

            except KeyboardInterrupt:
                logger.info("Stopped manually")
                break
            except Exception:
                logger.error("CRITICAL LOOP ERROR")
                logger.error(traceback.format_exc())
                time.sleep(10)


# ==================================================
# ENTRYPOINT
# ==================================================

def main():
    processor = OCRProcessor()
    processor.run()


if __name__ == "__main__":
    main()