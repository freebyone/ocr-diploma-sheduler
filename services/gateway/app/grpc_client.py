# app/grpc_client.py
from typing import Dict, Any, Optional, List
import asyncio
from grpclib.client import Channel
from grpclib.exceptions import GRPCError
import random
from dataclasses import dataclass
from contextlib import asynccontextmanager
from config import settings

@dataclass
class ServiceInstance:
    host: str
    port: int
    is_healthy: bool = True
    connections: int = 0

class GRPCClient:
    def __init__(self):
        self.services: Dict[str, List[ServiceInstance]] = {}
        self._channels: Dict[str, Channel] = {}
        self._current_index: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        """Инициализация подключений к сервисам"""
        for service_addr in settings.microservices:
            host, port = service_addr.split(":")
            service_name = host.split("-")[0]  # Извлекаем имя сервиса
            
            if service_name not in self.services:
                self.services[service_name] = []
                self._current_index[service_name] = 0
            
            self.services[service_name].append(
                ServiceInstance(host=host, port=int(port))
            )
    
    def _get_next_instance(self, service_name: str) -> Optional[ServiceInstance]:
        """Получение следующего инстанса сервиса (round-robin)"""
        if service_name not in self.services:
            return None
        
        instances = self.services[service_name]
        healthy_instances = [i for i in instances if i.is_healthy]
        
        if not healthy_instances:
            return None
        
        if settings.load_balancer == "round-robin":
            idx = self._current_index[service_name] % len(healthy_instances)
            self._current_index[service_name] += 1
            return healthy_instances[idx]
        elif settings.load_balancer == "random":
            return random.choice(healthy_instances)
        else:  # least-connections
            return min(healthy_instances, key=lambda x: x.connections)
    
    @asynccontextmanager
    async def get_channel(self, service_name: str):
        """Контекстный менеджер для получения канала gRPC"""
        instance = None
        
        async with self._lock:
            instance = self._get_next_instance(service_name)
            if instance:
                instance.connections += 1
        
        if not instance:
            raise Exception(f"No healthy instances for {service_name}")
        
        try:
            # Создаем или используем существующий канал
            key = f"{instance.host}:{instance.port}"
            if key not in self._channels:
                self._channels[key] = Channel(instance.host, instance.port)
            
            yield self._channels[key]
        finally:
            async with self._lock:
                if instance:
                    instance.connections -= 1
    
    async def call_service(self, service_name: str, method, request, retries=None):
        """Вызов метода gRPC с повторными попытками"""
        if retries is None:
            retries = settings.max_retries
        
        last_error = None
        
        for attempt in range(retries + 1):
            try:
                async with self.get_channel(service_name) as channel:
                    # Здесь будет специфичный вызов метода
                    # Например: await method(request, channel=channel)
                    return await self._execute_call(method, request, channel)
            except GRPCError as e:
                last_error = e
                if attempt < retries:
                    await asyncio.sleep(settings.retry_delay * (2 ** attempt))
                continue
            except Exception as e:
                last_error = e
                break
        
        raise last_error or Exception("Service call failed")
    
    async def _execute_call(self, method, request, channel):
        """Выполнение конкретного gRPC вызова"""
        # Этот метод будет переопределен для каждого сервиса
        pass
    
    async def health_check(self):
        """Проверка здоровья всех сервисов"""
        for service_name, instances in self.services.items():
            for instance in instances:
                try:
                    async with Channel(instance.host, instance.port) as channel:
                        # Простой ping или health check RPC
                        instance.is_healthy = await self._check_health(channel)
                except:
                    instance.is_healthy = False

grpc_client = GRPCClient()