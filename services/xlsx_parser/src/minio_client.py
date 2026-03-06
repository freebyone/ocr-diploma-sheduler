import os
import tempfile
from typing import List, Optional

from minio import Minio
from minio.error import S3Error

from config import settings


class MinioClient:
    """Клиент для работы с MinIO"""

    def __init__(self):
        self.client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self.bucket = settings.MINIO_BUCKET
        self.temp_dir = settings.TEMP_DIR
        os.makedirs(self.temp_dir, exist_ok=True)

    def ensure_bucket_exists(self) -> None:
        """Создать бакет если не существует"""
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            print(f"✅ Бакет '{self.bucket}' создан")
        else:
            print(f"ℹ️  Бакет '{self.bucket}' уже существует")

    def list_xlsx_files(self) -> List[str]:
        """Получить список xlsx-файлов из бакета"""
        files = []
        try:
            objects = self.client.list_objects(self.bucket, recursive=True)
            for obj in objects:
                name = obj.object_name
                if (
                    name.endswith(('.xlsx', '.xls'))
                    and not os.path.basename(name).startswith('~$')
                ):
                    files.append(name)
        except S3Error as e:
            print(f"❌ Ошибка при получении списка файлов из MinIO: {e}")
        return sorted(files)

    def download_file(self, object_name: str) -> Optional[str]:
        """
        Скачать файл из MinIO во временную директорию.
        Возвращает локальный путь к файлу.
        """
        # Сохраняем структуру подпапок если есть
        safe_name = object_name.replace("/", "_")
        local_path = os.path.join(self.temp_dir, safe_name)

        try:
            self.client.fget_object(self.bucket, object_name, local_path)
            print(f"   ⬇️  Скачан: {object_name} → {local_path}")
            return local_path
        except S3Error as e:
            print(f"❌ Ошибка скачивания {object_name}: {e}")
            return None

    def cleanup_temp_file(self, local_path: str) -> None:
        """Удалить временный файл"""
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except OSError:
            pass

    def cleanup_temp_dir(self) -> None:
        """Очистить всю временную директорию"""
        import shutil
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                os.makedirs(self.temp_dir, exist_ok=True)
        except OSError:
            pass