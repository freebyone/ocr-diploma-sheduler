from minio import Minio
from minio.error import S3Error
import io
import tempfile
import os
from app.config import config
import logging

logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(self):
        self.client = Minio(
            config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=config.MINIO_SECURE,
            region=config.MINIO_REGION
        )
        
    def list_images(self, prefix=None):
        try:
            objects = self.client.list_objects(
                config.MINIO_BUCKET,
                prefix=prefix,
                recursive=True
            )
            
            images = []
            for obj in objects:
                if obj.object_name.lower().endswith(('.jpg', '.jpeg', '.png')):
                    images.append({
                        'name': obj.object_name,
                        'size': obj.size,
                        'last_modified': obj.last_modified
                    })
            
            return images
            
        except S3Error as e:
            logger.error(f"Error listing images: {e}")
            return []
    
    def download_image(self, object_name):
        try:
            temp_file = tempfile.NamedTemporaryFile(
                suffix='.jpg', 
                delete=False,
                dir='/tmp'
            )
            temp_file.close()
            
            response = self.client.get_object(
                config.MINIO_BUCKET,
                object_name
            )
            
            with open(temp_file.name, 'wb') as f:
                for data in response.stream(amt=1024*1024):
                    f.write(data)
            
            logger.info(f"Downloaded image: {object_name} -> {temp_file.name}")
            return temp_file.name
            
        except S3Error as e:
            logger.error(f"Error downloading image {object_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading image: {e}")
            return None
        finally:
            if 'response' in locals():
                response.close()
                response.release_conn()
    
    def get_image_info(self, object_name):
        try:
            stat = self.client.stat_object(config.MINIO_BUCKET, object_name)
            return {
                'size': stat.size,
                'content_type': stat.content_type,
                'last_modified': stat.last_modified,
                'metadata': stat.metadata
            }
        except S3Error as e:
            logger.error(f"Error getting image info: {e}")
            return None
    
    def cleanup_temp_file(self, file_path):
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
                logger.debug(f"Cleaned up temp file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp file {file_path}: {e}")

minio_client = MinIOClient()