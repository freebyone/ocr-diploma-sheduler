#!/usr/bin/env python3
"""
MinIO Initialization Service
Аналог minio-init контейнера на Python
"""

import os
import json
import time
import logging
import sys
from typing import Dict, Optional

from minio import Minio
from minio.error import S3Error, ServerError

# Пытаемся импортировать AdminClient
try:
    from minio.admin import AdminClient
    ADMIN_SUPPORT = True
except ImportError:
    ADMIN_SUPPORT = False
    logger_import = logging.getLogger(__name__)
    logger_import.warning("AdminClient not available, user/policy management disabled")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinIOInitializer:
    """Класс для инициализации MinIO бакетов и политик"""
    
    def __init__(
        self,
        endpoint: str = "minio:9000",
        access_key: str = "ocrminio",
        secret_key: str = "admin123456",
        secure: bool = False
    ):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        
        # Инициализируем AdminClient если доступен
        self.admin_client = None
        if ADMIN_SUPPORT:
            try:
                self.admin_client = AdminClient(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure
                )
                logger.info("AdminClient initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize AdminClient: {e}")
        
        # Определение бакетов и их политик доступа
        self.buckets_config = {
            "xlsx-documents": "download",
            "documents-lite": "download",
            "xlsx-results": "download",
            "xlsx-errors": "download",
            "templates": "none",
            "results": "none",
            "errors": "none"
        }
        
        # Пользователи для создания
        self.users = {
            "appuser": "apppassword123"
        }
        
        # Политики доступа
        self.policies = {
            "readwrite": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::xlsx-documents/*",
                        "arn:aws:s3:::xlsx-results/*",
                        "arn:aws:s3:::xlsx-errors/*",
                        "arn:aws:s3:::documents-lite/*",
                        "arn:aws:s3:::templates/*",
                        "arn:aws:s3:::results/*",
                        "arn:aws:s3:::errors/*"
                    ]
                }]
            }
        }
    
    def wait_for_minio(self, max_retries: int = 30, delay: int = 2) -> bool:
        """Ожидание готовности MinIO сервера"""
        logger.info(f"Ожидание подключения к MinIO на {self.endpoint}...")
        
        for attempt in range(max_retries):
            try:
                # Проверяем подключение, пытаясь получить список бакетов
                self.client.list_buckets()
                logger.info("MinIO сервер готов к работе!")
                return True
            except (ServerError, ConnectionError) as e:
                logger.warning(f"Попытка {attempt + 1}/{max_retries}: MinIO недоступен. Ожидание {delay}с...")
                time.sleep(delay)
        
        logger.error("Не удалось подключиться к MinIO серверу")
        return False
    
    def create_buckets(self) -> None:
        """Создание бакетов если они не существуют"""
        logger.info("Создание бакетов...")
        
        for bucket_name in self.buckets_config.keys():
            try:
                found = self.client.bucket_exists(bucket_name)
                if not found:
                    self.client.make_bucket(bucket_name)
                    logger.info(f"✓ Бакет '{bucket_name}' создан")
                else:
                    logger.info(f"✓ Бакет '{bucket_name}' уже существует")
            except S3Error as e:
                logger.error(f"✗ Ошибка при создании бакета '{bucket_name}': {e}")
                raise
    
    def set_bucket_policies(self) -> None:
        """Установка политик доступа для бакетов"""
        logger.info("Настройка публичного доступа к бакетам...")
        
        for bucket_name, policy_type in self.buckets_config.items():
            try:
                if policy_type == "none":
                    # Для политики "none" просто удаляем политику с бакета
                    try:
                        self.client.delete_bucket_policy(bucket_name)
                        logger.info(f"✓ Политика 'none' применена к бакету '{bucket_name}' (публичный доступ отключен)")
                    except S3Error as e:
                        if e.code == "NoSuchBucketPolicy":
                            # Если политики и так нет, это нормально
                            logger.info(f"✓ Бакет '{bucket_name}' уже не имеет публичной политики")
                        else:
                            raise
                            
                elif policy_type == "download":
                    # Создаем политику для публичного скачивания
                    policy = {
                        "Version": "2012-10-17",
                        "Statement": [{
                            "Effect": "Allow",
                            "Principal": {"AWS": ["*"]},
                            "Action": ["s3:GetObject"],
                            "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                        }]
                    }
                    
                    policy_json = json.dumps(policy)
                    self.client.set_bucket_policy(bucket_name, policy_json)
                    logger.info(f"✓ Политика 'download' применена к бакету '{bucket_name}'")
                else:
                    logger.warning(f"✗ Неизвестный тип политики '{policy_type}' для бакета '{bucket_name}'")
                    
            except S3Error as e:
                logger.error(f"✗ Ошибка при установке политики для '{bucket_name}': {e}")
                raise
    
    def create_users(self) -> None:
        """Создание пользователей (требует административных привилегий)"""
        if not self.admin_client:
            logger.warning("AdminClient не доступен. Пропускаем создание пользователей.")
            return
        
        logger.info("Создание пользователей...")
        
        for username, password in self.users.items():
            try:
                # Проверяем существует ли пользователь
                try:
                    self.admin_client.get_user(username)
                    logger.info(f"✓ Пользователь '{username}' уже существует")
                except Exception as e:
                    # Пользователь не существует, создаем
                    try:
                        self.admin_client.add_user(username, password)
                        logger.info(f"✓ Пользователь '{username}' создан")
                    except Exception as add_error:
                        logger.error(f"✗ Ошибка при создании пользователя '{username}': {add_error}")
            except Exception as e:
                logger.error(f"✗ Ошибка при проверке пользователя '{username}': {e}")
    
    def create_policies(self) -> None:
        """Создание кастомных политик и привязка к пользователям"""
        if not self.admin_client:
            logger.warning("AdminClient не доступен. Пропускаем создание политик.")
            return
        
        logger.info("Создание кастомных политик...")
        
        for policy_name, policy_definition in self.policies.items():
            try:
                policy_json = json.dumps(policy_definition, indent=2)
                
                # Создаем или обновляем политику
                try:
                    # Проверяем существует ли политика
                    existing_policy = self.admin_client.get_policy(policy_name)
                    if existing_policy:
                        logger.info(f"✓ Политика '{policy_name}' уже существует")
                        # Можно обновить если нужно
                        # self.admin_client.add_canned_policy(policy_name, policy_json)
                    else:
                        self.admin_client.add_canned_policy(policy_name, policy_json)
                        logger.info(f"✓ Политика '{policy_name}' создана")
                except Exception as e:
                    # Политика не найдена или другая ошибка
                    try:
                        self.admin_client.add_canned_policy(policy_name, policy_json)
                        logger.info(f"✓ Политика '{policy_name}' создана")
                    except Exception as add_error:
                        logger.error(f"✗ Ошибка при создании политики '{policy_name}': {add_error}")
                        continue
                
                # Привязываем политику к пользователям
                for username in self.users.keys():
                    try:
                        # Получаем текущие политики пользователя
                        user_info = self.admin_client.get_user(username)
                        
                        # Привязываем политику
                        self.admin_client.attach_policy(policy_name, username)
                        logger.info(f"✓ Политика '{policy_name}' привязана к пользователю '{username}'")
                    except Exception as e:
                        logger.error(f"✗ Ошибка при привязке политики к '{username}': {e}")
                        
            except Exception as e:
                logger.error(f"✗ Ошибка при обработке политики '{policy_name}': {e}")
    
    def verify_setup(self) -> None:
        """Верификация созданной конфигурации"""
        logger.info("Верификация созданной конфигурации...")
        
        # Проверяем бакеты
        buckets = self.client.list_buckets()
        bucket_names = [b.name for b in buckets]
        
        for required_bucket in self.buckets_config.keys():
            if required_bucket in bucket_names:
                logger.info(f"✓ Бакет '{required_bucket}' присутствует")
            else:
                logger.error(f"✗ Бакет '{required_bucket}' отсутствует!")
        
        # Проверяем пользователей если admin доступен
        if self.admin_client:
            for username in self.users.keys():
                try:
                    user_info = self.admin_client.get_user(username)
                    if user_info:
                        logger.info(f"✓ Пользователь '{username}' существует и активен")
                    else:
                        logger.warning(f"⚠ Пользователь '{username}' не найден")
                except Exception as e:
                    logger.warning(f"⚠ Не удалось проверить пользователя '{username}': {e}")
    
    def run(self) -> None:
        """Основной метод запуска инициализации"""
        logger.info("=" * 60)
        logger.info("Запуск MinIO инициализации")
        logger.info("=" * 60)
        
        # Ожидаем готовности MinIO
        if not self.wait_for_minio():
            logger.error("MinIO не готов, выход с ошибкой")
            sys.exit(1)
        
        try:
            # Создаем бакеты
            self.create_buckets()
            
            # Устанавливаем политики
            self.set_bucket_policies()
            
            # Создаем пользователей (если доступно)
            self.create_users()
            
            # Создаем и применяем политики (если доступно)
            self.create_policies()
            
            # Верифицируем настройки
            self.verify_setup()
            
            logger.info("=" * 60)
            logger.info("✓ MinIO инициализация успешно завершена!")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"✗ Ошибка при инициализации MinIO: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    """Точка входа для Docker контейнера"""
    # Параметры можно передать через переменные окружения
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "ocrminio")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin123456")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    
    # Дополнительное ожидание для синхронизации
    initial_delay = int(os.getenv("INITIAL_DELAY", "3"))
    logger.info(f"Ожидание {initial_delay} секунд перед началом...")
    time.sleep(initial_delay)
    
    # Запускаем инициализацию
    initializer = MinIOInitializer(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    initializer.run()


if __name__ == "__main__":
    main()