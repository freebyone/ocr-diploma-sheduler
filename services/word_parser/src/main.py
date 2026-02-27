from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import logging
from sqlalchemy.orm import Session
import io
from docx import Document

from config import get_settings
from database import get_db, init_db
from parser import WordParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    debug=settings.DEBUG,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    logger.info("Запуск приложения")
    init_db()


@app.post("/api/parse-word")
async def parse_word_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:

    if not file.filename.endswith(".docx"):
        raise HTTPException(status_code=400, detail="Только .docx")

    content = await file.read()
    doc = Document(io.BytesIO(content))

    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]

    tables = []
    for table in doc.tables:
        table_data = []
        for row in table.rows:
            row_data = [cell.text.strip() for cell in row.cells]
            table_data.append(row_data)
        tables.append(table_data)

    logger.info(f"Абзацев: {len(paragraphs)}")
    logger.info(f"Таблиц: {len(tables)}")

    parser = WordParser(db)
    stats = parser.parse_document(paragraphs, tables)

    return {
        "success": True,
        "stats": stats,
    }

@app.post("/api/parse-multiple-word")
async def parse_multiple_word_files(
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db),
):

    results = []
    total_stats = {
        "universities": 0,
        "directions": 0,
        "specializations": 0,
        "study_programs": 0,
        "control_tables": 0,
        "errors": [],
    }

    for file in files:
        try:
            content = await file.read()
            doc = Document(io.BytesIO(content))

            paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]

            tables = []
            for table in doc.tables:
                table_data = []
                for row in table.rows:
                    row_data = [cell.text.strip() for cell in row.cells]
                    table_data.append(row_data)
                tables.append(table_data)

            parser = WordParser(db)
            stats = parser.parse_document(paragraphs, tables)

            results.append({
                "filename": file.filename,
                "success": True,
                "stats": stats
            })

            for key in total_stats:
                if key != "errors":
                    total_stats[key] += stats.get(key, 0)
            total_stats["errors"].extend(stats.get("errors", []))

        except Exception as e:
            results.append({
                "filename": file.filename,
                "success": False,
                "error": str(e)
            })
            total_stats["errors"].append(str(e))

    return {
        "success": True,
        "total_files": len(files),
        "successful": len([r for r in results if r["success"]]),
        "failed": len([r for r in results if not r["success"]]),
        "total_stats": total_stats,
        "results": results,
    }