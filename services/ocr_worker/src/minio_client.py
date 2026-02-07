import io
import logging
from typing import List, Dict, Any, Optional
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = False
    ):
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
        self.ensure_bucket_exists()
    
    def ensure_bucket_exists(self):
        """Убедиться, что bucket существует"""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Bucket {self.bucket_name} создан")
        except S3Error as e:
            logger.error(f"Ошибка при создании bucket: {e}")
    
    def check_connection(self) -> bool:
        """Проверка подключения к MinIO"""
        try:
            self.client.list_buckets()
            logger.info("✓ Подключение к MinIO установлено")
            return True
        except Exception as e:
            logger.error(f"✗ Ошибка подключения к MinIO: {e}")
            return False
    
    def get_unprocessed_folders(self, processed_folder: str = "processed") -> List[str]:
        """Получить список необработанных папок (исключая processed)"""
        try:
            objects = self.client.list_objects(
                self.bucket_name,
                recursive=False
            )
            
            folders = []
            for obj in objects:
                if obj.is_dir:
                    folder_name = obj.object_name.rstrip('/')
                    # Пропускаем папку processed и пустые папки
                    if folder_name != processed_folder and folder_name:
                        folders.append(folder_name)
            
            # Сортируем по времени создания (по имени, если это UUID)
            folders.sort()
            logger.info(f"Найдено {len(folders)} необработанных папок")
            return folders
            
        except S3Error as e:
            logger.error(f"Ошибка при получении папок: {e}")
            return []
    
    def list_images_in_folder(self, folder_path: str) -> List[Dict[str, Any]]:
        """Получить список изображений в папке"""
        try:
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=f"{folder_path}/",
                recursive=True
            )
            
            images = []
            for obj in objects:
                if not obj.is_dir and obj.object_name.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    images.append({
                        'name': obj.object_name,
                        'size': obj.size,
                        'last_modified': obj.last_modified.isoformat() if obj.last_modified else None
                    })
            
            # Сортируем по имени (чтобы 1.jpg, 2.jpg и т.д.)
            images.sort(key=lambda x: x['name'])
            return images
            
        except S3Error as e:
            logger.error(f"Ошибка при получении изображений: {e}")
            return []
    
    def download_image(self, object_name: str) -> Optional[bytes]:
        """Скачать изображение из MinIO"""
        try:
            response = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            image_data = response.read()
            response.close()
            response.release_conn()
            
            logger.debug(f"Изображение {object_name} загружено ({len(image_data)} bytes)")
            return image_data
            
        except S3Error as e:
            logger.error(f"Ошибка при загрузке изображения {object_name}: {e}")
            return None
    
    def move_folder_to_processed(self, folder_name: str) -> bool:
        """Переместить всю папку в processed"""
        try:
            # Создаем новое имя для папки в processed
            destination_prefix = f"processed/{folder_name}/"
            
            # Получаем все объекты в папке
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=f"{folder_name}/",
                recursive=True
            )
            
            moved_count = 0
            for obj in objects:
                if not obj.is_dir:
                    source_name = obj.object_name
                    # Создаем destination имя (сохраняем структуру)
                    relative_path = source_name[len(folder_name)+1:]
                    destination_name = f"{destination_prefix}{relative_path}"
                    
                    # Копируем файл
                    try:
                        # Способ 1: для новых версий MinIO
                        from minio.commonconfig import CopySource
                        self.client.copy_object(
                            bucket_name=self.bucket_name,
                            object_name=destination_name,
                            source=CopySource(self.bucket_name, source_name)
                        )
                    except (ImportError, TypeError):
                        # Способ 2: ручное копирование
                        image_data = self.download_image(source_name)
                        if image_data:
                            self.client.put_object(
                                bucket_name=self.bucket_name,
                                object_name=destination_name,
                                data=io.BytesIO(image_data),
                                length=len(image_data),
                                content_type=self._get_content_type(source_name)
                            )
                    
                    # Удаляем оригинал
                    self.client.remove_object(
                        bucket_name=self.bucket_name,
                        object_name=source_name
                    )
                    moved_count += 1
            
            # Удаляем пустую исходную папку
            try:
                # Папки в MinIO - это виртуальные, но удалим если есть
                self.client.remove_object(
                    bucket_name=self.bucket_name,
                    object_name=f"{folder_name}/"
                )
            except:
                pass
            
            logger.info(f"✓ Папка {folder_name} перемещена в processed ({moved_count} файлов)")
            return True
            
        except Exception as e:
            logger.error(f"✗ Ошибка при перемещении папки {folder_name}: {e}")
            return False
    
    def _get_content_type(self, filename: str) -> str:
        """Определить content type по расширению файла"""
        if filename.lower().endswith('.jpg') or filename.lower().endswith('.jpeg'):
            return 'image/jpeg'
        elif filename.lower().endswith('.png'):
            return 'image/png'
        elif filename.lower().endswith('.webp'):
            return 'image/webp'
        else:
            return 'application/octet-stream'