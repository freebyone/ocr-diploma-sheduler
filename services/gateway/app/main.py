# app/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import asyncio
import logging
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from starlette.middleware.cors import CORSMiddleware

from websocket import ws_handler
from grpc_client import grpc_client
from metrics import metrics
from config import settings

# Настройка логирования
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    logger.info("Starting Gateway...")
    
    # Инициализация gRPC клиента
    await grpc_client.initialize()
    
    # Запуск метрик
    if settings.metrics_enabled:
        metrics.start_server()
        logger.info(f"Metrics server started on port {settings.metrics_port}")
    
    # Запуск фоновых задач
    health_task = asyncio.create_task(health_check_loop())
    
    yield
    
    # Остановка
    logger.info("Shutting down Gateway...")
    health_task.cancel()
    
    try:
        await health_task
    except asyncio.CancelledError:
        pass

# Создание FastAPI приложения
app = FastAPI(title="WebSocket Gateway", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok", "service": "websocket-gateway"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get("/metrics")
async def get_metrics():
    """Endpoint для Prometheus"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint"""
    await ws_handler.handle_connection(websocket)

@app.websocket("/ws/{client_id}")
async def websocket_with_id(websocket: WebSocket, client_id: str):
    """WebSocket endpoint с указанным client_id"""
    await ws_handler.handle_connection(websocket)

async def health_check_loop():
    """Фоновая задача для проверки здоровья сервисов"""
    while True:
        try:
            await grpc_client.health_check()
            
            # Обновление метрик активных соединений
            ws_count = len(ws_handler.manager.active_connections)
            grpc_count = sum(
                sum(i.connections for i in instances) 
                for instances in grpc_client.services.values()
            )
            metrics.update_active_connections(ws_count, grpc_count)
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        
        await asyncio.sleep(30)  # Проверка каждые 30 секунд

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        host=settings.ws_host,
        port=settings.ws_port,
        reload=True,
        log_level=settings.log_level.lower(),
        ws_ping_interval=20,
        ws_ping_timeout=20
    )