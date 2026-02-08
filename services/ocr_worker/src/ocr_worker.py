import requests
import base64
import time
import os
import tempfile
import logging
from minio import Minio
from minio.error import S3Error
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        
        self.source_bucket = "documents"
        self.results_bucket = "results"
        self.errors_bucket = "errors"
        
        self._ensure_buckets()
    
    def _ensure_buckets(self):
        for bucket in [self.source_bucket, self.results_bucket, self.errors_bucket]:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
    
    def list_folders(self):
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
    
    def list_images(self, folder):
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
    
    def download_image(self, image_path):
        try:
            temp_file = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            temp_path = temp_file.name
            temp_file.close()
            
            self.minio_client.fget_object(self.source_bucket, image_path, temp_path)
            return temp_path
        except Exception as e:
            logger.error(f"Error downloading {image_path}: {e}")
            return None
    
    def process_single_image(self, image_path):
        local_path = self.download_image(image_path)
        if not local_path:
            return {
                "image_path": image_path,
                "error": "Failed to download image",
                "status": "error"
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
                
                if done_reason == 'stop':
                    logger.info(f"Success in {elapsed:.1f}s (reason: {done_reason}), text length: {len(ocr_text)}")
                    
                    return {
                        "image_path": image_path,
                        "ocr_text": ocr_text,
                        "processing_time": elapsed,
                        "model": self.model,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "done_reason": done_reason,
                        "eval_count": result.get('eval_count', 0),
                        "total_duration": result.get('total_duration', 0),
                        "status": "success"
                    }
                else:
                    logger.warning(f"Processing stopped with reason: {done_reason} in {elapsed:.1f}s")
                    
                    return {
                        "image_path": image_path,
                        "ocr_text": ocr_text,
                        "processing_time": elapsed,
                        "model": self.model,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "done_reason": done_reason,
                        "eval_count": result.get('eval_count', 0),
                        "total_duration": result.get('total_duration', 0),
                        "status": "partial" if ocr_text else "error"
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
            return {
                "image_path": image_path,
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "error"
            }
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
    
    def save_to_bucket(self, bucket_name, folder, data, filename_suffix="result"):
        try:
            json_data = json.dumps(data, indent=2, ensure_ascii=False)
            
            result_filename = f"{folder}/ocr_{filename_suffix}.json"
            
            import io
            data_stream = io.BytesIO(json_data.encode('utf-8'))
            data_length = len(json_data)
            
            self.minio_client.put_object(
                bucket_name,
                result_filename,
                data_stream,
                data_length,
                content_type="application/json"
            )
            
            logger.info(f"Saved to {bucket_name}/{result_filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to {bucket_name}: {e}")
            return False
    
    def move_folder_to_errors(self, folder, results):
        try:
            error_data = {
                "folder": folder,
                "moved_to_errors_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": "Contains failed/partial OCR results",
                "results": results
            }
            
            self.save_to_bucket(self.errors_bucket, folder, error_data, "errors")
            
            prefix = f"{folder}/"
            objects = self.minio_client.list_objects(self.source_bucket, prefix=prefix, recursive=True)
            
            for obj in objects:
                self.minio_client.copy_object(
                    self.errors_bucket,
                    obj.object_name,
                    f"{self.source_bucket}/{obj.object_name}"
                )
            
            logger.info(f"Moved folder {folder} to errors bucket")
            return True
            
        except Exception as e:
            logger.error(f"Error moving folder to errors: {e}")
            return False
    
    def delete_folder_from_source(self, folder):
        try:
            objects_to_delete = []
            prefix = f"{folder}/"
            
            objects = self.minio_client.list_objects(self.source_bucket, prefix=prefix, recursive=True)
            for obj in objects:
                objects_to_delete.append(obj.object_name)
            
            if objects_to_delete:
                errors = self.minio_client.remove_objects(self.source_bucket, objects_to_delete)
                for err in errors:
                    logger.error(f"Error deleting object: {err}")
                
                logger.info(f"Deleted folder {folder} with {len(objects_to_delete)} objects")
                return True
            else:
                logger.warning(f"Folder {folder} is empty or doesn't exist")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting folder {folder}: {e}")
            return False
    
    def analyze_results(self, results):
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
            "all_success_stop": success_with_stop == total,
            "has_length_issues": success_with_length > 0 or partial > 0,
            "has_errors": errors > 0
        }
        
        return analysis
    
    def process_folder(self, folder):
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
            
            self.save_to_bucket(self.results_bucket, folder, result_data, "result")
            
            logger.info(f"Deleting folder from source: {folder}")
            self.delete_folder_from_source(folder)
            
            return True
            
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
                        try:
                            self.move_folder_to_errors(folder, [{"error": str(e)}])
                            self.delete_folder_from_source(folder)
                        except:
                            pass
                
                if not folders:
                    logger.debug("No folders found. Checking again in 30 seconds...")
                
                time.sleep(30)
                
            except KeyboardInterrupt:
                logger.info("Stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(30)

def main():
    processor = OCRProcessor()
    processor.run()

if __name__ == "__main__":
    main()