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

# Увеличиваем лимиты для JSON
sys.setrecursionlimit(1000000)  # Увеличиваем для больших JSON

logger = logging.getLogger(__name__)

class OCRProcessor:
    def __init__(self):
        # Проверка версии MinIO
        try:
            from importlib.metadata import version
            minio_version = version("minio")
            logger.info(f"MinIO library version: {minio_version}")
        except:
            try:
                import pkg_resources
                minio_version = pkg_resources.get_distribution("minio").version
                logger.info(f"MinIO library version: {minio_version}")
            except:
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
        for bucket in [self.source_bucket, self.results_bucket, self.errors_bucket]:
            try:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except Exception as e:
                logger.error(f"Error ensuring bucket {bucket}: {e}")
    
    def list_folders(self) -> List[str]:
        folders = []
        try:
            objects = self.minio_client.list_objects(self.source_bucket, recursive=False)
            for obj in objects:
                if obj.object_name.endswith('/'):
                    folder_name = obj.object_name.rstrip('/')
                    folders.append(folder_name)
        except Exception as e:
            logger.error(f"Error listing folders: {e}")
        return folders
    
    def list_images(self, folder: str) -> List[str]:
        images = []
        try:
            prefix = f"{folder}/"
            objects = self.minio_client.list_objects(self.source_bucket, prefix=prefix, recursive=True)
            for obj in objects:
                if not obj.object_name.endswith('/'):
                    images.append(obj.object_name)
        except Exception as e:
            logger.error(f"Error listing images in {folder}: {e}")
        return images
    
    def download_image(self, image_path: str) -> Optional[str]:
        try:
            temp_file = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            temp_path = temp_file.name
            temp_file.close()
            
            self.minio_client.fget_object(self.source_bucket, image_path, temp_path)
            return temp_path
        except Exception as e:
            logger.error(f"Error downloading {image_path}: {e}")
            return None
    
    def process_single_image(self, image_path: str) -> Dict[str, Any]:
        local_path = self.download_image(image_path)
        if not local_path:
            return {
                "image_path": image_path,
                "error": "Failed to download image",
                "status": "error",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        
        try:
            with open(local_path, "rb") as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')
            
            prompt = "<|grounding|>Convert the document to markdown."
            
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
            
            logger.info(f"Sending to Ollama: {image_path}")
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
                
                logger.info(f"{'Success' if status == 'success' else 'Partial'} in {elapsed:.1f}s "
                           f"(reason: {done_reason}), text length: {len(ocr_text)}")
                
                return {
                    "image_path": image_path,
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
                    "image_path": image_path,
                    "error": f"HTTP {response.status_code}",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "error"
                }
                
        except requests.exceptions.Timeout:
            logger.error(f"Timeout processing {image_path} after 600s")
            return {
                "image_path": image_path,
                "error": "Timeout after 600 seconds",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "error"
            }
        except Exception as e:
            logger.error(f"Error processing {image_path}: {e}")
            logger.error(traceback.format_exc())
            return {
                "image_path": image_path,
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "error"
            }
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
    
    def save_to_bucket(self, bucket_name: str, folder: str, data: Dict, filename_suffix: str = "result") -> bool:
        temp_file_path = None
        try:
            result_filename = f"{folder}/ocr_{filename_suffix}.json"
            
            # Создаем временный файл
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', encoding='utf-8', delete=False) as temp_file:
                temp_file_path = temp_file.name
                # Записываем JSON с правильными параметрами
                json.dump(data, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())  # Принудительная запись на диск
            
            # Проверяем размер файла
            file_size = os.path.getsize(temp_file_path)
            logger.info(f"JSON file size: {file_size / 1024:.2f} KB")
            
            # Загружаем в MinIO
            with open(temp_file_path, 'rb') as file_data:
                self.minio_client.put_object(
                    bucket_name,
                    result_filename,
                    file_data,
                    file_size,
                    content_type="application/json; charset=utf-8"
                )
            
            logger.info(f"Saved to {bucket_name}/{result_filename}")
            
            # Проверяем загруженный файл
            self._verify_saved_file(bucket_name, result_filename, file_size)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving to {bucket_name}: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            # Удаляем временный файл
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass
    
    def _verify_saved_file(self, bucket: str, filename: str, expected_size: int) -> bool:
        try:
            # Получаем информацию о файле
            obj_info = self.minio_client.stat_object(bucket, filename)
            actual_size = obj_info.size
            
            if actual_size == expected_size:
                logger.info(f"File verification passed: {filename} ({actual_size} bytes)")
                return True
            else:
                logger.error(f"File size mismatch for {filename}: expected {expected_size}, got {actual_size}")
                return False
        except Exception as e:
            logger.error(f"Error verifying file {filename}: {e}")
            return False
    
    def move_folder_to_errors(self, folder: str, results: List[Dict]) -> bool:
        try:
            error_data = {
                "folder": folder,
                "moved_to_errors_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": "Contains failed/partial OCR results",
                "results": results
            }
            
            self.save_to_bucket(self.errors_bucket, folder, error_data, "errors")
            
            # Копируем файлы в errors bucket
            prefix = f"{folder}/"
            objects = self.minio_client.list_objects(self.source_bucket, prefix=prefix, recursive=True)
            
            copy_success = True
            for obj in objects:
                try:
                    self.minio_client.copy_object(
                        self.errors_bucket,
                        obj.object_name,
                        f"{self.source_bucket}/{obj.object_name}"
                    )
                except Exception as e:
                    logger.error(f"Error copying {obj.object_name}: {e}")
                    copy_success = False
            
            if copy_success:
                logger.info(f"Moved folder {folder} to errors bucket")
            else:
                logger.warning(f"Some files failed to copy for folder {folder}")
            
            return copy_success
            
        except Exception as e:
            logger.error(f"Error moving folder to errors: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def delete_folder_from_source(self, folder: str) -> bool:
        try:
            logger.info(f"=== Starting deletion of folder: {folder} ===")
            
            prefix = f"{folder}/"
            logger.info(f"Looking for objects with prefix: {prefix}")
            
            # Получаем список объектов
            objects = []
            try:
                objects_iter = self.minio_client.list_objects(
                    self.source_bucket, 
                    prefix=prefix, 
                    recursive=True
                )
                objects = list(objects_iter)
                logger.info(f"Found {len(objects)} objects to delete")
                
            except Exception as e:
                logger.error(f"Error listing objects: {e}")
                return False
            
            if not objects:
                logger.info(f"No objects found in folder {folder}")
                return True
            
            # Удаляем объекты по одному
            success_count = 0
            error_count = 0
            
            for obj in objects:
                try:
                    obj_name = obj.object_name
                    self.minio_client.remove_object(self.source_bucket, obj_name)
                    success_count += 1
                    logger.debug(f"Deleted: {obj_name}")
                except Exception as e:
                    logger.error(f"Error deleting object {obj_name}: {e}")
                    error_count += 1
            
            logger.info(f"Deletion summary for {folder}: {success_count} succeeded, {error_count} failed")
            
            return error_count == 0
                
        except Exception as e:
            logger.error(f"Unexpected error in delete_folder_from_source: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def analyze_results(self, results: List[Dict]) -> Dict[str, Any]:
        total = len(results)
        success = len([r for r in results if r.get("status") == "success"])
        partial = len([r for r in results if r.get("status") == "partial"])
        errors = len([r for r in results if r.get("status") == "error"])
        
        success_with_stop = 0
        success_with_length = 0
        
        for result in results:
            if result.get("status") == "success":
                if result.get("done_reason") == "stop":
                    success_with_stop += 1
                elif result.get("done_reason") == "length":
                    success_with_length += 1
        
        analysis = {
            "total_images": total,
            "success_stop": success_with_stop,
            "success_length": success_with_length,
            "partial": partial,
            "errors": errors,
            "all_success_stop": success_with_stop == total and total > 0,
            "has_length_issues": success_with_length > 0 or partial > 0,
            "has_errors": errors > 0
        }
        
        return analysis
    
    def process_folder(self, folder: str) -> bool:
        logger.info(f"=== Processing folder: {folder} ===")
        
        images = self.list_images(folder)
        
        if not images:
            logger.warning(f"No images found in folder: {folder}")
            self.delete_folder_from_source(folder)
            return False
        
        logger.info(f"Found {len(images)} images to process")
        
        results = []
        
        for idx, img_path in enumerate(images, 1):
            logger.info(f"[{idx}/{len(images)}] Processing: {img_path}")
            
            result = self.process_single_image(img_path)
            if result:
                results.append(result)
            
            time.sleep(1)
        
        analysis = self.analyze_results(results)
        logger.info(f"Analysis: {analysis}")
        
        if analysis["all_success_stop"]:
            logger.info(f"All images processed successfully with reason=stop")
            
            result_data = {
                "folder": folder,
                "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "analysis": analysis,
                "results": results
            }
            
            save_success = self.save_to_bucket(self.results_bucket, folder, result_data, "result")
            
            if save_success:
                logger.info(f"Deleting folder from source: {folder}")
                delete_success = self.delete_folder_from_source(folder)
                
                if delete_success:
                    logger.info(f"Successfully deleted source folder: {folder}")
                else:
                    logger.error(f"Failed to delete source folder: {folder}")
                
                return True
            else:
                logger.error(f"Failed to save results for folder {folder}")
                return False
            
        elif analysis["has_length_issues"] or analysis["has_errors"]:
            logger.warning(f"Folder has issues. Moving to errors bucket")
            
            self.move_folder_to_errors(folder, results)
            
            logger.info(f"Deleting folder from source: {folder}")
            self.delete_folder_from_source(folder)
            
            return False
        
        else:
            logger.error(f"Unexpected analysis result: {analysis}")
            self.move_folder_to_errors(folder, results)
            self.delete_folder_from_source(folder)
            return False
    
    def run(self):
        logger.info("=== Starting OCR Processor ===")
        
        # Проверка подключения к MinIO
        try:
            buckets = self.minio_client.list_buckets()
            logger.info(f"Successfully connected to MinIO. Available buckets: {[b.name for b in buckets]}")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            logger.error("Check if MinIO is running and credentials are correct")
            return
        
        logger.info(f"Source bucket: {self.source_bucket}")
        logger.info(f"Results bucket: {self.results_bucket}")
        logger.info(f"Errors bucket: {self.errors_bucket}")
        logger.info("Waiting for folders to process...")
        logger.info("Rules:")
        logger.info("  - All images with 'done_reason: stop' → results bucket")
        logger.info("  - Any image with 'done_reason: length' or error → errors bucket")
        logger.info("  - Source folder deleted after processing")
        
        while True:
            try:
                folders = self.list_folders()
                
                if folders:
                    logger.info(f"Found {len(folders)} folders to process")
                
                for folder in folders:
                    try:
                        self.process_folder(folder)
                    except Exception as e:
                        logger.error(f"Error processing folder {folder}: {e}")
                        logger.error(traceback.format_exc())
                        try:
                            self.move_folder_to_errors(folder, [{"error": str(e)}])
                            self.delete_folder_from_source(folder)
                        except Exception as inner_e:
                            logger.error(f"Error in error handling for {folder}: {inner_e}")
                
                if not folders:
                    logger.debug("No folders found. Checking again in 30 seconds...")
                
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