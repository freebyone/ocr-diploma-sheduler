import os
import io
import re
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import fitz
from minio import Minio
from minio.error import S3Error
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "ocrminio"
MINIO_SECRET_KEY = "admin123456"
MINIO_SECURE = False
MINIO_BUCKET = "documents-lite"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

try:
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        logger.info(f"Bucket {MINIO_BUCKET} создан")
except S3Error as e:
    logger.error(f"Ошибка при создании bucket: {e}")

app = FastAPI(title="PDF Processor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ProcessingResult(BaseModel):
    pdf_name: str
    extracted_prefix: str
    image_object: str
    bucket: str
    timestamp: str
    message: str


class ErrorResponse(BaseModel):
    error: str
    detail: str
    timestamp: str


def extract_prefix_from_filename(filename: str) -> str:
    """
    Извлекает числовой префикс из имени файла.
    
    Примеры:
        '0001_Scan_приложение_юристы.pdf' -> '0001'
        '0102_Scan_приложение_юристы.pdf' -> '0102'
        '42_document.pdf' -> '42'
    
    Если префикс не найден — выбрасывает HTTPException.
    """
    # Берём имя без расширения
    base_name = os.path.splitext(filename)[0]

    # Ищем числовой префикс до первого подчёркивания
    match = re.match(r'^(\d+)', base_name)

    if not match:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Не удалось извлечь числовой префикс из имени файла '{filename}'. "
                f"Ожидается формат вида '0001_описание.pdf'"
            )
        )

    return match.group(1)


def pdf_first_page_to_image(
    pdf_content: bytes,
    pdf_filename: str
) -> ProcessingResult:
    """
    Конвертирует ТОЛЬКО первую страницу PDF в JPEG
    и сохраняет в MinIO с именем, полученным из префикса файла.
    """
    # 1. Извлекаем префикс
    prefix = extract_prefix_from_filename(pdf_filename)
    image_object_name = f"{prefix}.jpg"

    try:
        # 2. Открываем PDF
        pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
        total_pages = len(pdf_document)

        logger.info(
            f"Обработка PDF '{pdf_filename}': {total_pages} страниц, "
            f"префикс='{prefix}'"
        )

        if total_pages == 0:
            pdf_document.close()
            return ProcessingResult(
                pdf_name=pdf_filename,
                extracted_prefix=prefix,
                image_object="",
                bucket=MINIO_BUCKET,
                timestamp=datetime.now().isoformat(),
                message="PDF файл не содержит страниц"
            )

        # 3. Берём ТОЛЬКО первую страницу (индекс 0)
        page = pdf_document.load_page(0)

        # 4. Рендерим в изображение (2x масштаб ≈ 144 DPI)
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        img_data = pix.tobytes("jpeg")

        pdf_document.close()

        # 5. Загружаем в MinIO
        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=image_object_name,
            data=io.BytesIO(img_data),
            length=len(img_data),
            content_type="image/jpeg"
        )

        logger.info(
            f"Изображение первой страницы сохранено: "
            f"{MINIO_BUCKET}/{image_object_name}"
        )

        return ProcessingResult(
            pdf_name=pdf_filename,
            extracted_prefix=prefix,
            image_object=image_object_name,
            bucket=MINIO_BUCKET,
            timestamp=datetime.now().isoformat(),
            message=(
                f"Первая страница '{pdf_filename}' сохранена как "
                f"'{image_object_name}' в бакет '{MINIO_BUCKET}'"
            )
        )

    except HTTPException:
        raise
    except S3Error as e:
        logger.error(f"Ошибка MinIO при загрузке '{image_object_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка загрузки в MinIO: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Ошибка при обработке PDF '{pdf_filename}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка обработки PDF: {str(e)}"
        )


# ────────────────────────── ENDPOINTS ──────────────────────────

@app.get("/")
async def root():
    return {"message": "PDF Processor API", "status": "running"}


@app.get("/health")
async def health_check():
    try:
        minio_client.list_buckets()
        return {
            "status": "healthy",
            "minio": "connected",
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "minio": "disconnected",
            "error": str(e),
        }


@app.post("/api/process-pdf", response_model=ProcessingResult)
async def process_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """
    Загружает PDF, извлекает первую страницу как JPEG
    и сохраняет в MinIO с именем на основе числового префикса файла.
    
    Пример:
        Загружен: 0001_Scan_приложение_юристы.pdf
        Результат в MinIO: documents/0001.jpg
    """
    try:
        if not file.filename.lower().endswith('.pdf'):
            raise HTTPException(
                status_code=400,
                detail="Файл должен быть в формате PDF"
            )

        contents = await file.read()

        if len(contents) == 0:
            raise HTTPException(
                status_code=400,
                detail="PDF файл пустой"
            )

        result = pdf_first_page_to_image(contents, file.filename)
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера: {str(e)}"
        )


@app.post("/api/process-multiple-pdfs")
async def process_multiple_pdfs(files: List[UploadFile] = File(...)):
    """
    Пакетная обработка нескольких PDF файлов.
    Для каждого файла извлекается первая страница.
    """
    results = []
    errors = []

    for file in files:
        try:
            if not file.filename.lower().endswith('.pdf'):
                errors.append({
                    "filename": file.filename,
                    "error": "Не PDF файл"
                })
                continue

            contents = await file.read()

            if len(contents) == 0:
                errors.append({
                    "filename": file.filename,
                    "error": "PDF файл пустой"
                })
                continue

            result = pdf_first_page_to_image(contents, file.filename)
            results.append(result.dict())

        except HTTPException as e:
            errors.append({
                "filename": file.filename,
                "error": e.detail
            })
        except Exception as e:
            errors.append({
                "filename": file.filename,
                "error": str(e)
            })

    return {
        "processed": len(results),
        "failed": len(errors),
        "results": results,
        "errors": errors
    }


@app.get("/api/list-images")
async def list_all_images():
    """
    Выводит список всех изображений в бакете.
    """
    try:
        objects = minio_client.list_objects(
            MINIO_BUCKET,
            recursive=True
        )

        images = []
        for obj in objects:
            if obj.object_name.lower().endswith('.jpg'):
                images.append({
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": (
                        obj.last_modified.isoformat()
                        if obj.last_modified else None
                    )
                })

        return {
            "bucket": MINIO_BUCKET,
            "images_count": len(images),
            "images": images
        }

    except S3Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при получении списка объектов: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)