# app/metrics.py
from prometheus_client import start_http_server, Counter, Histogram, Gauge
import asyncio
from typing import Optional

class MetricsCollector:
    def __init__(self, port: int = 9090):
        self.port = port
        
        # Дополнительные метрики
        self.GRPC_CALLS = Counter('grpc_calls_total', 'Total gRPC calls', ['service', 'method', 'status'])
        self.GRPC_DURATION = Histogram('grpc_duration_seconds', 'gRPC call duration', ['service', 'method'])
        self.ACTIVE_WS = Gauge('websocket_active_connections', 'Active WebSocket connections')
        self.ACTIVE_GRPC = Gauge('grpc_active_connections', 'Active gRPC connections')
        
        self._server_started = False
    
    def start_server(self):
        """Запуск HTTP сервера для метрик"""
        if not self._server_started:
            start_http_server(self.port)
            self._server_started = True
    
    def record_grpc_call(self, service: str, method: str, duration: float, success: bool = True):
        """Запись метрик gRPC вызова"""
        status = "success" if success else "error"
        self.GRPC_CALLS.labels(service=service, method=method, status=status).inc()
        self.GRPC_DURATION.labels(service=service, method=method).observe(duration)
    
    def update_active_connections(self, ws_count: int, grpc_count: int):
        """Обновление счетчиков активных соединений"""
        self.ACTIVE_WS.set(ws_count)
        self.ACTIVE_GRPC.set(grpc_count)

metrics = MetricsCollector()