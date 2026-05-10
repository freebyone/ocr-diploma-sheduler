#!/usr/bin/env python3
"""
MinIO Initialization Service
Аналог minio-init контейнера на Python
"""

import os
import json
import time
import logging
from typing import List, Dict, Any

from minio import Minio
from minio.error import S3Error, ServerError

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
                "Version": "2025-10-17",
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
        
        # Соответствие между политиками и их конфигурацией
        policy_configs = {
            "none": {
                "Version": "2012-10-17",
                "Statement": []
            },
            "download": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:aws:s3:::{}/*"]
                }]
            },
            "upload": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:PutObject"],
                    "Resource": ["arn:aws:s3:::{}/*"]
                }]
            },
            "public": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": ["arn:aws:s3:::{}/*"]
                }]
            }
        }
        
        for bucket_name, policy_type in self.buckets_config.items():
            try:
                if policy_type == "none":
                    # Удаляем политику
                    self.client.set_bucket_policy(bucket_name, "")
                    logger.info(f"✓ Политика 'none' применена к бакету '{bucket_name}'")
                elif policy_type in policy_configs:
                    # Создаем политику с правильным Resource
                    policy = policy_configs[policy_type].copy()
                    policy["Statement"][0]["Resource"] = [
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                    
                    policy_json = json.dumps(policy)
                    self.client.set_bucket_policy(bucket_name, policy_json)
                    logger.info(f"✓ Политика '{policy_type}' применена к бакету '{bucket_name}'")
                else:
                    logger.warning(f"✗ Неизвестный тип политики '{policy_type}' для бакета '{bucket_name}'")
            except S3Error as e:
                logger.error(f"✗ Ошибка при установке политики для '{bucket_name}': {e}")
                raise
    
    def create_users(self) -> None:
        """Создание пользователей (требует административных привилегий)"""
        logger.info("Создание пользователей...")
        
        # Проверяем, является ли текущий пользователь администратором
        try:
            # Пытаемся выполнить admin операцию
            self.client.list_users()
        except S3Error as e:
            if e.code == "AccessDenied":
                logger.warning("Текущий пользователь не имеет прав администратора. Пропускаем создание пользователей.")
                return
            raise
        
        for username, password in self.users.items():
            try:
                # Проверяем существует ли пользователь
                try:
                    self.client.get_user_info(username)
                    logger.info(f"✓ Пользователь '{username}' уже существует")
                except S3Error:
                    # Создаем нового пользователя
                    self.client.add_user(username, password)
                    logger.info(f"✓ Пользователь '{username}' создан")
            except S3Error as e:
                logger.error(f"✗ Ошибка при создании пользователя '{username}': {e}")
    
    def create_policies(self) -> None:
        """Создание кастомных политик и привязка к пользователям"""
        logger.info("Создание кастомных политик...")
        
        # Проверяем права администратора
        try:
            self.client.list_policies()
        except S3Error as e:
            if e.code == "AccessDenied":
                logger.warning("Текущий пользователь не имеет прав администратора. Пропускаем создание политик.")
                return
            raise
        
        for policy_name, policy_definition in self.policies.items():
            try:
                # Проверяем существует ли политика
                try:
                    self.client.get_policy(policy_name)
                    logger.info(f"✓ Политика '{policy_name}' уже существует")
                    
                    # Обновляем если нужно (опционально)
                    # self.client.set_policy(policy_name, json.dumps(policy_definition))
                except S3Error:
                    # Создаем новую политику
                    policy_json = json.dumps(policy_definition)
                    self.client.set_policy(policy_name, policy_json)
                    logger.info(f"✓ Политика '{policy_name}' создана")
            except S3Error as e:
                logger.error(f"✗ Ошибка при создании политики '{policy_name}': {e}")
        
        # Привязываем политики к пользователям
        if "readwrite" in self.policies:
            for username in self.users.keys():
                try:
                    # Получаем текущие политики пользователя
                    user_info = self.client.get_user_info(username)
                    
                    # Добавляем политику
                    current_policies = user_info.get("policy", "").split(",")
                    if "readwrite" not in current_policies:
                        # Применяем политику (это может перезаписать существующие)
                        all_policies = [p for p in current_policies if p] + ["readwrite"]
                        self.client.set_user_policy(username, ",".join(all_policies))
                        logger.info(f"✓ Политика 'readwrite' привязана к пользователю '{username}'")
                    else:
                        logger.info(f"✓ Политика 'readwrite' уже привязана к '{username}'")
                except S3Error as e:
                    logger.error(f"✗ Ошибка при привязке политики к '{username}': {e}")
    
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
        
        # Проверяем политики бакетов
        for bucket_name in self.buckets_config.keys():
            try:
                policy = self.client.get_bucket_policy(bucket_name)
                if policy:
                    logger.info(f"✓ Бакет '{bucket_name}' имеет настроенную политику")
                else:
                    logger.info(f"✓ Бакет '{bucket_name}' имеет политику 'none'")
            except S3Error:
                logger.info(f"✓ Бакет '{bucket_name}' не имеет публичной политики")
    
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
            
            # Создаем пользователей
            self.create_users()
            
            # Создаем и применяем политики
            self.create_policies()
            
            # Верифицируем настройки
            self.verify_setup()
            
            logger.info("=" * 60)
            logger.info("✓ MinIO инициализация успешно завершена!")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"✗ Ошибка при инициализации MinIO: {e}")
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
    import sys
    main()