# app/websocket.py
import json
import asyncio
import uuid
from typing import Dict, Optional
from fastapi import WebSocket, WebSocketDisconnect
from prometheus_client import Counter, Histogram
from grpc_client import grpc_client

# Метрики
WS_CONNECTIONS = Counter('websocket_connections_total', 'Total WebSocket connections')
WS_MESSAGES = Counter('websocket_messages_total', 'Total WebSocket messages')
WS_ERRORS = Counter('websocket_errors_total', 'Total WebSocket errors')
REQUEST_DURATION = Histogram('request_duration_seconds', 'Request duration in seconds')

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_data: Dict[str, Dict] = {}
    
    async def connect(self, websocket: WebSocket, client_id: str = None):
        await websocket.accept()
        
        if not client_id:
            client_id = str(uuid.uuid4())
        
        self.active_connections[client_id] = websocket
        self.connection_data[client_id] = {
            "connected_at": asyncio.get_event_loop().time(),
            "last_activity": asyncio.get_event_loop().time(),
            "ip": websocket.client.host if websocket.client else "unknown"
        }
        
        WS_CONNECTIONS.inc()
        return client_id
    
    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]
            del self.connection_data[client_id]
    
    async def send_message(self, client_id: str, message: dict):
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_json(message)
                self.connection_data[client_id]["last_activity"] = asyncio.get_event_loop().time()
            except Exception as e:
                WS_ERRORS.inc()
                raise
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections.values():
            try:
                await connection.send_json(message)
            except:
                pass

class WebSocketHandler:
    def __init__(self):
        self.manager = ConnectionManager()
        self.message_handlers = {}
        
        # Регистрация обработчиков сообщений
        self._register_handlers()
    
    def _register_handlers(self):
        self.message_handlers["auth"] = self._handle_auth
        self.message_handlers["chat_message"] = self._handle_chat_message
        self.message_handlers["get_user"] = self._handle_get_user
    
    async def handle_connection(self, websocket: WebSocket):
        client_id = None
        
        try:
            client_id = await self.manager.connect(websocket)
            
            # Отправляем client_id клиенту
            await self.manager.send_message(client_id, {
                "type": "connection_established",
                "client_id": client_id
            })
            
            # Основной цикл обработки сообщений
            while True:
                try:
                    # Таймаут для проверки активности
                    data = await asyncio.wait_for(
                        websocket.receive_json(),
                        timeout=300  # 5 минут
                    )
                    
                    await self._process_message(client_id, data)
                    
                except asyncio.TimeoutError:
                    # Проверка активности соединения
                    await websocket.send_json({"type": "ping"})
                    try:
                        pong = await asyncio.wait_for(
                            websocket.receive_json(),
                            timeout=10
                        )
                        if pong.get("type") != "pong":
                            break
                    except asyncio.TimeoutError:
                        break
                        
        except WebSocketDisconnect:
            pass
        except Exception as e:
            WS_ERRORS.inc()
            print(f"WebSocket error: {e}")
        finally:
            if client_id:
                self.manager.disconnect(client_id)
    
    async def _process_message(self, client_id: str, data: dict):
        message_type = data.get("type")
        request_id = data.get("request_id", str(uuid.uuid4()))
        
        WS_MESSAGES.inc()
        
        # Обработка через прометеус
        with REQUEST_DURATION.time():
            try:
                if message_type in self.message_handlers:
                    handler = self.message_handlers[message_type]
                    response = await handler(client_id, data)
                    
                    # Отправляем ответ
                    await self.manager.send_message(client_id, {
                        "type": f"{message_type}_response",
                        "request_id": request_id,
                        "data": response,
                        "success": True
                    })
                else:
                    raise Exception(f"Unknown message type: {message_type}")
                    
            except Exception as e:
                WS_ERRORS.inc()
                # Отправляем ошибку клиенту
                await self.manager.send_message(client_id, {
                    "type": "error",
                    "request_id": request_id,
                    "error": str(e),
                    "success": False
                })
    
    async def _handle_auth(self, client_id: str, data: dict):
        """Пример обработки аутентификации"""
        token = data.get("token")
        
        # Вызов сервиса аутентификации через gRPC
        # response = await grpc_client.call_service(
        #     "user-service",
        #     "authenticate",
        #     {"token": token}
        # )
        
        # Заглушка
        return {"authenticated": True, "user_id": "123"}
    
    async def _handle_chat_message(self, client_id: str, data: dict):
        """Пример обработки сообщения чата"""
        message = data.get("message")
        room_id = data.get("room_id")
        
        # 1. Сохраняем в сервисе чата
        # await grpc_client.call_service("chat-service", "save_message", {
        #     "room_id": room_id,
        #     "message": message,
        #     "sender": client_id
        # })
        
        # 2. Рассылаем другим участникам
        # await self.manager.broadcast({
        #     "type": "new_message",
        #     "room_id": room_id,
        #     "message": message,
        #     "sender": client_id
        # })
        
        return {"status": "sent", "room_id": room_id}
    
    async def _handle_get_user(self, client_id: str, data: dict):
        """Пример получения данных пользователя"""
        user_id = data.get("user_id")
        
        # Вызов user-service через gRPC
        # user_data = await grpc_client.call_service(
        #     "user-service",
        #     "get_user",
        #     {"user_id": user_id}
        # )
        
        # Заглушка
        return {"id": user_id, "name": "John Doe"}

ws_handler = WebSocketHandler()