import os
import shutil
import io
import fitz  # PyMuPDF
import logging
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from collections import defaultdict
from tempfile import TemporaryDirectory
from sqlalchemy.orm import Session

from database import init_db, get_db, check_db_connection
from models import PairCounter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Unified Processor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "ocrminio"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "admin123456"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
)


@app.on_event("startup")
async def startup_event():
    """Инициализация БД при старте приложения."""
    logger.info("Initializing database...")
    init_db()
    logger.info("Startup complete.")


def get_surname(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    if name.endswith('.plx'):
        name = name[:-4]
    return name.split()[0].split('_')[0]


@app.get("/health")
async def health():
    db_ok = check_db_connection()
    return {
        "status": "ok",
        "database": "connected" if db_ok else "unavailable",
    }


@app.post("/api/process")
async def process_data(
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    for bucket in ["xlsx-documents", "documents-lite"]:
        if not MINIO_CLIENT.bucket_exists(bucket):
            MINIO_CLIENT.make_bucket(bucket)

    results_count = 0

    try:
        with TemporaryDirectory() as temp_dir:
            stored_filenames = []
            for file in files:
                target_path = os.path.join(temp_dir, file.filename)
                with open(target_path, "wb") as f:
                    shutil.copyfileobj(file.file, f)
                stored_filenames.append(file.filename)

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

                    # Значения для записи в БД
                    xlsx_fname = None
                    pdf_fname = None
                    minio_xlsx_key = None
                    minio_jpg_key = None

                    # --- XLSX ---
                    if i < len(xlsx):
                        xlsx_fname = xlsx[i]
                        path = os.path.join(temp_dir, xlsx_fname)
                        minio_xlsx_key = f"{prefix}_{xlsx_fname}"
                        with open(path, "rb") as f:
                            MINIO_CLIENT.put_object(
                                "xlsx-documents",
                                minio_xlsx_key,
                                f,
                                os.path.getsize(path),
                                content_type="application/vnd.ms-excel",
                            )
                        results_count += 1

                    # --- PDF → JPG ---
                    if i < len(pdfs):
                        pdf_fname = pdfs[i]
                        path = os.path.join(temp_dir, pdf_fname)
                        minio_jpg_key = f"{prefix}.jpg"
                        try:
                            with fitz.open(path) as doc:
                                if len(doc) > 0:
                                    page = doc.load_page(0)
                                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                                    img_data = pix.tobytes("jpeg")
                                    MINIO_CLIENT.put_object(
                                        "documents-lite",
                                        minio_jpg_key,
                                        io.BytesIO(img_data),
                                        len(img_data),
                                        content_type="image/jpeg",
                                    )
                                    results_count += 1
                        except Exception as e:
                            logger.error(f"Error PDF {pdf_fname}: {e}")
                            minio_jpg_key = None  # не записываем битый ключ

                    # --- Запись в БД ---
                    record = PairCounter(
                        pair_index=pair_idx,
                        surname=surname,
                        xlsx_filename=xlsx_fname,
                        pdf_filename=pdf_fname,
                        minio_xlsx_key=minio_xlsx_key,
                        minio_jpg_key=minio_jpg_key,
                    )
                    db.add(record)

                    pair_idx += 1

            db.commit()

        return {"status": "success", "processed_count": results_count}

    except Exception as e:
        db.rollback()
        logger.error(f"Processing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        for file in files:
            await file.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)