import os
import shutil
import io
import fitz  # PyMuPDF
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from collections import defaultdict
from tempfile import TemporaryDirectory

app = FastAPI(title="Unified Processor")

# Настройки CORS для фронтенда
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Настройки MinIO из окружения
MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "ocrminio"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "admin123456"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
)

def get_surname(filename):
    name = os.path.splitext(filename)[0]
    if name.endswith('.plx'): name = name[:-4]
    return name.split()[0].split('_')[0]

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/api/process")
async def process_data(files: List[UploadFile] = File(...)):
    # Создаем бакеты если их нет
    for b in ["xlsx-documents", "documents-lite"]:
        if not MINIO_CLIENT.bucket_exists(b):
            MINIO_CLIENT.make_bucket(b)

    results_count = 0
    
    try:
        # TemporaryDirectory автоматически удалится после выхода из блока with
        with TemporaryDirectory() as temp_dir:
            
            # 1. Сбрасываем все файлы на диск (защита RAM от OOM при 500+ файлах)
            stored_filenames = []
            for file in files:
                target_path = os.path.join(temp_dir, file.filename)
                with open(target_path, "wb") as f:
                    shutil.copyfileobj(file.file, f)
                stored_filenames.append(file.filename)

            # 2. Группируем имена по фамилиям
            groups = defaultdict(list)
            for name in stored_filenames:
                groups[get_surname(name)].append(name)

            pair_idx = 1
            for surname in sorted(groups.keys()):
                f_list = groups[surname]
                xlsx = sorted([f for f in f_list if f.lower().endswith(('.xlsx', '.xls'))])
                pdfs = sorted([f for f in f_list if f.lower().endswith('.pdf')])

                for i in range(max(len(xlsx), len(pdfs))):
                    prefix = f"{pair_idx:03d}"
                    
                    # Грузим Excel
                    if i < len(xlsx):
                        fname = xlsx[i]
                        path = os.path.join(temp_dir, fname)
                        with open(path, "rb") as f:
                            MINIO_CLIENT.put_object(
                                "xlsx-documents", f"{prefix}_{fname}", 
                                f, os.path.getsize(path),
                                content_type="application/vnd.ms-excel"
                            )
                        results_count += 1

                    # Грузим PDF (делаем превью)
                    if i < len(pdfs):
                        fname = pdfs[i]
                        path = os.path.join(temp_dir, fname)
                        try:
                            with fitz.open(path) as doc:
                                if len(doc) > 0:
                                    page = doc.load_page(0)
                                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                                    img_data = pix.tobytes("jpeg")
                                    MINIO_CLIENT.put_object(
                                        "documents-lite", f"{prefix}.jpg",
                                        io.BytesIO(img_data), len(img_data),
                                        content_type="image/jpeg"
                                    )
                                    results_count += 1
                        except Exception as e:
                            print(f"Error processing PDF {fname}: {e}")
                    
                    pair_idx += 1
                    
            return {"status": "success", "processed_count": results_count}

    except Exception as e:
        print(f"Critical error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Явное закрытие дескрипторов FastAPI
        for file in files:
            await file.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)