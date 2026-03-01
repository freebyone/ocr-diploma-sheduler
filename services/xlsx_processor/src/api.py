from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import List
import os
import tempfile
import logging
from src.parser import parse_xlsx_file  # Измененный импорт

app = FastAPI(title="Excel Parser API", description="API для парсинга Excel файлов с учебными планами")

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL для подключения к БД (можно вынести в переменные окружения)
DB_URL = os.getenv("DB_URL", "postgresql://postgres:12345678@postgres:5432/norma_db")

@app.post("/api/parse-excel")
async def parse_excel(file: UploadFile = File(...)):
    """
    Парсинг одного Excel файла
    """
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="Файл должен быть в формате .xlsx")
    
    # Сохраняем файл временно
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        # Парсим файл
        result = parse_xlsx_file(tmp_path, DB_URL)
        
        return JSONResponse({
            "filename": file.filename,
            "success": result['specialization'] is not None,
            "specialization": result['specialization'],
            "direction": result['direction'],
            "study_programs": result['study_programs'],
            "format_controls": result['format_controls'],
            "format_retests": result['format_retests'],
            "control_tables": result['control_tables']
        })
    except Exception as e:
        logger.error(f"Ошибка при парсинге файла {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")
    finally:
        # Удаляем временный файл
        os.unlink(tmp_path)

@app.post("/api/parse-multiple-excel")
async def parse_multiple_excel(files: List[UploadFile] = File(...)):
    """
    Парсинг нескольких Excel файлов
    """
    results = []
    total_stats = {
        'directions': 0,
        'specializations': 0,
        'study_programs': 0,
        'format_controls': 0,
        'format_retests': 0,
        'control_tables': 0
    }
    errors = []
    
    for file in files:
        if not file.filename.endswith('.xlsx'):
            results.append({
                "filename": file.filename,
                "success": False,
                "error": "Файл должен быть в формате .xlsx"
            })
            continue
        
        # Сохраняем файл временно
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        try:
            # Парсим файл
            result = parse_xlsx_file(tmp_path, DB_URL)
            
            file_result = {
                "filename": file.filename,
                "success": result['specialization'] is not None,
                "specialization": result['specialization'],
                "direction": result['direction'],
                "study_programs": result['study_programs'],
                "format_controls": result['format_controls'],
                "format_retests": result['format_retests'],
                "control_tables": result['control_tables']
            }
            
            results.append(file_result)
            
            # Обновляем общую статистику
            if result['direction']:
                total_stats['directions'] += 1
            if result['specialization']:
                total_stats['specializations'] += 1
            total_stats['study_programs'] += result['study_programs']
            total_stats['format_controls'] += result['format_controls']
            total_stats['format_retests'] += result['format_retests']
            total_stats['control_tables'] += result['control_tables']
            
        except Exception as e:
            error_msg = f"Ошибка парсинга: {str(e)}"
            logger.error(f"Ошибка при парсинге файла {file.filename}: {str(e)}")
            results.append({
                "filename": file.filename,
                "success": False,
                "error": error_msg
            })
            errors.append(f"{file.filename}: {error_msg}")
        finally:
            # Удаляем временный файл
            os.unlink(tmp_path)
    
    return JSONResponse({
        "total": len(files),
        "processed": len([r for r in results if r.get('success')]),
        "failed": len([r for r in results if not r.get('success')]),
        "results": results,
        "total_stats": total_stats,
        "errors": errors
    })

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "excel-parser"}