"""
Сервис загрузки Excel-файлов в MinIO.
Принимает .xlsx через API, сохраняет в бакет xlsx-documents.
"""

import os
import io
import re
from typing import List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from minio import Minio
from minio.error import S3Error

# ══════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "ocrminio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "xlsx-documents")


# ══════════════════════════════════════════════
#  MINIO КЛИЕНТ
# ══════════════════════════════════════════════

def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket: str):
    """Создать бакет если не существует"""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"✅ Бакет '{bucket}' создан")


# ══════════════════════════════════════════════
#  МОДЕЛИ ОТВЕТОВ
# ══════════════════════════════════════════════

class UploadResult(BaseModel):
    filename: str
    object_name: str
    bucket: str
    size: int
    success: bool
    error: str | None = None


class UploadResponse(BaseModel):
    message: str
    processed: int
    failed: int
    results: List[UploadResult]
    errors: List[str]


# ══════════════════════════════════════════════
#  FASTAPI ПРИЛОЖЕНИЕ
# ══════════════════════════════════════════════

app = FastAPI(
    title="Excel Upload to MinIO",
    description="Загрузка Excel-файлов в MinIO бакет xlsx-documents",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    """При запуске — убедиться что бакет существует"""
    try:
        client = get_minio_client()
        ensure_bucket(client, MINIO_BUCKET)
        print(f"✅ Подключение к MinIO: {MINIO_ENDPOINT}")
        print(f"✅ Бакет: {MINIO_BUCKET}")
    except Exception as e:
        print(f"⚠️ MinIO недоступен при старте: {e}")


@app.get("/health")
async def health():
    """Проверка здоровья сервиса"""
    try:
        client = get_minio_client()
        bucket_exists = client.bucket_exists(MINIO_BUCKET)
        return {
            "status": "ok",
            "minio_connected": True,
            "bucket_exists": bucket_exists,
            "bucket": MINIO_BUCKET,
        }
    except Exception as e:
        return {
            "status": "degraded",
            "minio_connected": False,
            "error": str(e),
        }


@app.get("/api/list-files")
async def list_files():
    """Список файлов в бакете"""
    try:
        client = get_minio_client()
        objects = client.list_objects(MINIO_BUCKET, recursive=True)
        files = []
        for obj in objects:
            files.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat()
                if obj.last_modified else None,
            })
        return {"bucket": MINIO_BUCKET, "files": files, "count": len(files)}
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}")


@app.post("/api/upload-excel", response_model=UploadResponse)
async def upload_excel_files(files: List[UploadFile] = File(...)):
    """
    Загрузить один или несколько Excel-файлов в MinIO.
    Файлы сохраняются в бакет xlsx-documents с оригинальными именами.
    """
    if not files:
        raise HTTPException(status_code=400, detail="Файлы не переданы")

    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    results: List[UploadResult] = []
    errors: List[str] = []
    processed = 0
    failed = 0

    for upload_file in files:
        filename = upload_file.filename or "unknown.xlsx"

        # ── Валидация расширения ──
        if not filename.lower().endswith(('.xlsx', '.xls')):
            error_msg = f"{filename}: не Excel-файл"
            errors.append(error_msg)
            results.append(UploadResult(
                filename=filename,
                object_name="",
                bucket=MINIO_BUCKET,
                size=0,
                success=False,
                error=error_msg,
            ))
            failed += 1
            continue

        try:
            # ── Читаем содержимое файла ──
            content = await upload_file.read()
            file_size = len(content)

            if file_size == 0:
                error_msg = f"{filename}: пустой файл"
                errors.append(error_msg)
                results.append(UploadResult(
                    filename=filename,
                    object_name="",
                    bucket=MINIO_BUCKET,
                    size=0,
                    success=False,
                    error=error_msg,
                ))
                failed += 1
                continue

            # ── Имя объекта в MinIO = оригинальное имя файла ──
            object_name = filename

            # ── Проверяем, не существует ли уже ──
            try:
                existing = client.stat_object(MINIO_BUCKET, object_name)
                # Файл уже есть — перезаписываем
                print(
                    f"   ⚠️ Файл '{object_name}' уже существует "
                    f"в бакете, перезаписываем"
                )
            except S3Error:
                # Файла нет — это нормально
                pass

            # ── Загружаем в MinIO ──
            data_stream = io.BytesIO(content)
            client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                data=data_stream,
                length=file_size,
                content_type=(
                    "application/vnd.openxmlformats-officedocument"
                    ".spreadsheetml.sheet"
                ),
            )

            print(f"   ✅ Загружен: {object_name} ({file_size} байт)")

            results.append(UploadResult(
                filename=filename,
                object_name=object_name,
                bucket=MINIO_BUCKET,
                size=file_size,
                success=True,
            ))
            processed += 1

        except S3Error as e:
            error_msg = f"{filename}: ошибка MinIO — {e}"
            errors.append(error_msg)
            results.append(UploadResult(
                filename=filename,
                object_name="",
                bucket=MINIO_BUCKET,
                size=0,
                success=False,
                error=error_msg,
            ))
            failed += 1
            print(f"   ❌ {error_msg}")

        except Exception as e:
            error_msg = f"{filename}: {str(e)}"
            errors.append(error_msg)
            results.append(UploadResult(
                filename=filename,
                object_name="",
                bucket=MINIO_BUCKET,
                size=0,
                success=False,
                error=error_msg,
            ))
            failed += 1
            print(f"   ❌ {error_msg}")

        finally:
            await upload_file.close()

    return UploadResponse(
        message=f"Загрузка завершена: {processed} успешно, {failed} ошибок",
        processed=processed,
        failed=failed,
        results=results,
        errors=errors,
    )


@app.delete("/api/delete-file/{filename:path}")
async def delete_file(filename: str):
    """Удалить файл из бакета"""
    try:
        client = get_minio_client()
        client.remove_object(MINIO_BUCKET, filename)
        return {"message": f"Файл '{filename}' удалён", "success": True}
    except S3Error as e:
        raise HTTPException(
            status_code=404,
            detail=f"Файл не найден или ошибка: {e}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)