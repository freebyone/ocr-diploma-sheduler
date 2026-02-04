import os
import io
import tempfile
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import fitz
from PIL import Image
from minio import Minio
from minio.error import S3Error
import uuid
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "ocrminio"
MINIO_SECRET_KEY = "admin123456"
MINIO_SECURE = False
MINIO_BUCKET = "pdf-images"

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

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ProcessingResult(BaseModel):
    pdf_id: str
    pdf_name: str
    pages_count: int
    images_extracted: int
    bucket: str
    minio_objects: List[str]
    timestamp: str
    message: str

class ErrorResponse(BaseModel):
    error: str
    detail: str
    timestamp: str

def pdf_to_images(pdf_content: bytes, pdf_id: str, pdf_filename: str) -> ProcessingResult:
    """
    Извлекает изображения из PDF и загружает их в MinIO
    """
    images_extracted = 0
    minio_objects = []
    
    try:
        # Открываем PDF из байтов
        pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
        total_pages = len(pdf_document)
        
        logger.info(f"Обработка PDF '{pdf_filename}': {total_pages} страниц")
        
        if total_pages == 0:
            pdf_document.close()
            return ProcessingResult(
                pdf_id=pdf_id,
                pdf_name=pdf_filename,
                pages_count=0,
                images_extracted=0,
                bucket=MINIO_BUCKET,
                minio_objects=[],
                timestamp=datetime.now().isoformat(),
                message="PDF файл не содержит страниц"
            )
        
        # Обрабатываем каждую страницу
        for page_num in range(total_pages):
            page = pdf_document.load_page(page_num)
            
            # Получаем изображение страницы
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # Увеличиваем DPI для качества
            img_data = pix.tobytes("jpeg")
            
            # Создаем имя файла для изображения
            image_name = f"{pdf_id}/{page_num + 1}.jpg"
            
            # Загружаем в MinIO
            try:
                minio_client.put_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=image_name,
                    data=io.BytesIO(img_data),
                    length=len(img_data),
                    content_type="image/jpeg"
                )
                images_extracted += 1
                minio_objects.append(image_name)
                logger.info(f"Изображение сохранено: {image_name}")
                
            except S3Error as e:
                logger.error(f"Ошибка при загрузке в MinIO: {e}")
        
        pdf_document.close()
        
        return ProcessingResult(
            pdf_id=pdf_id,
            pdf_name=pdf_filename,
            pages_count=total_pages,
            images_extracted=images_extracted,
            bucket=MINIO_BUCKET,
            minio_objects=minio_objects,
            timestamp=datetime.now().isoformat(),
            message=f"Успешно извлечено {images_extracted} изображений из {total_pages} страниц"
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка обработки PDF: {str(e)}")

@app.get("/")
async def root():
    return {"message": "PDF Processor API", "status": "running"}

@app.get("/health")
async def health_check():
    try:
        # Проверяем соединение с MinIO
        minio_client.list_buckets()
        return {"status": "healthy", "minio": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "minio": "disconnected", "error": str(e)}

@app.post("/api/process-pdf", response_model=ProcessingResult)
async def process_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """
    Обрабатывает PDF файл: извлекает изображения с каждой страницы и сохраняет в MinIO
    """
    try:
        # Проверяем тип файла
        if not file.filename.lower().endswith('.pdf'):
            raise HTTPException(
                status_code=400,
                detail="Файл должен быть в формате PDF"
            )
        
        # Читаем содержимое файла
        contents = await file.read()
        
        if len(contents) == 0:
            raise HTTPException(
                status_code=400,
                detail="PDF файл пустой"
            )
        
        # Генерируем уникальный ID для PDF
        pdf_id = str(uuid.uuid4())
        
        # Обрабатываем PDF
        result = pdf_to_images(contents, pdf_id, file.filename)
        
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
    Обрабатывает несколько PDF файлов
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
            
            pdf_id = str(uuid.uuid4())
            result = pdf_to_images(contents, pdf_id, file.filename)
            results.append(result.dict())
            
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

@app.get("/api/list-images/{pdf_id}")
async def list_pdf_images(pdf_id: str):
    """
    Получает список изображений для конкретного PDF
    """
    try:
        objects = minio_client.list_objects(
            MINIO_BUCKET,
            prefix=f"{pdf_id}/",
            recursive=True
        )
        
        images = []
        for obj in objects:
            images.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None
            })
        
        return {
            "pdf_id": pdf_id,
            "images_count": len(images),
            "images": images
        }
        
    except S3Error as e:
        raise HTTPException(status_code=404, detail=f"PDF с ID {pdf_id} не найден")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)