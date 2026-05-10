import os
import shutil
import uuid
import fitz
import io
from typing import List
from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from minio import Minio
from collections import defaultdict
from tempfile import TemporaryDirectory

# ... (конфигурация Minio остается прежней)

@app.post("/api/process")
async def process_files(files: List[UploadFile] = File(...)):
    ensure_buckets()
    
    # Создаем уникальную временную папку для этой сессии загрузки
    with TemporaryDirectory() as temp_dir:
        # 1. Сохраняем файлы на диск, чтобы не забивать RAM
        temp_file_paths = []
        for file in files:
            file_path = os.path.join(temp_dir, file.filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            temp_file_paths.append(file.filename)

        # 2. Группируем имена файлов
        surname_files = defaultdict(list)
        for fname in temp_file_paths:
            surname = get_surname(fname)
            surname_files[surname].append(fname)
        
        results = []
        pair_number = 1

        # 3. Обрабатываем по очереди (один за другим)
        for surname in sorted(surname_files.keys()):
            f_list = surname_files[surname]
            xlsx_list = sorted([f for f in f_list if f.lower().endswith(('.xlsx', '.xls'))])
            pdf_list = sorted([f for f in f_list if f.lower().endswith('.pdf')])
            
            num_pairs = max(len(xlsx_list), len(pdf_list))
            
            for i in range(num_pairs):
                prefix = f"{pair_number:03d}"
                
                # Обработка XLSX
                if i < len(xlsx_list):
                    old_name = xlsx_list[i]
                    new_name = f"{prefix}_{old_name}"
                    full_path = os.path.join(temp_dir, old_name)
                    
                    # Отправляем в MinIO напрямую с диска
                    with open(full_path, "rb") as f:
                        file_stat = os.stat(full_path)
                        minio_client.put_object(
                            BUCKET_XLSX, new_name, f, file_stat.st_size,
                            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    results.append({"original": old_name, "renamed": new_name})

                # Обработка PDF
                if i < len(pdf_list):
                    old_name = pdf_list[i]
                    full_path = os.path.join(temp_dir, old_name)
                    
                    try:
                        # Открываем PDF с диска
                        with fitz.open(full_path) as pdf_doc:
                            if len(pdf_doc) > 0:
                                page = pdf_doc.load_page(0)
                                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                                img_data = pix.tobytes("jpeg")
                                
                                image_name = f"{prefix}.jpg"
                                minio_client.put_object(
                                    BUCKET_PDF_LITE, image_name, io.BytesIO(img_data), len(img_data),
                                    content_type="image/jpeg"
                                )
                                results.append({"original": old_name, "renamed": image_name})
                    except Exception as e:
                        print(f"Ошибка в PDF {old_name}: {e}")

                pair_number += 1

        return {"status": "success", "processed_count": len(results)}