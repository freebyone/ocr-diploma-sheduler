import os
import io
import zipfile
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from collections import defaultdict

app = FastAPI(title="File Renamer Service")

def get_surname(filename):
    """Извлекает фамилию из названия файла"""
    name = os.path.splitext(filename)[0]
    if name.endswith('.plx'):
        name = name[:-4]
    surname = name.split()[0].split('_')[0]
    return surname

def process_files(files_dict: dict):
    """
    Принимает словарь {имя_файла: содержимое}, 
    возвращает словарь {новое_имя: содержимое}
    """
    # Группируем имена файлов по фамилии
    surname_files = defaultdict(list)
    for filename in files_dict.keys():
        surname = get_surname(filename)
        surname_files[surname].append(filename)
    
    renamed_files = {}
    pair_number = 1
    
    for surname in sorted(surname_files.keys()):
        file_list = surname_files[surname]
        
        xlsx_files = sorted([f for f in file_list if f.lower().endswith('.xlsx')])
        pdf_files = sorted([f for f in file_list if f.lower().endswith('.pdf')])
        
        num_pairs = max(len(xlsx_files), len(pdf_files))
        
        for i in range(num_pairs):
            prefix = f"{pair_number:03d}_"
            
            if i < len(xlsx_files):
                old_name = xlsx_files[i]
                renamed_files[prefix + old_name] = files_dict[old_name]
            
            if i < len(pdf_files):
                old_name = pdf_files[i]
                renamed_files[prefix + old_name] = files_dict[old_name]
            
            pair_number += 1
            
    return renamed_files

@app.post("/rename-zip")
async def rename_zip(file: UploadFile = File(...)):
    """Принимает ZIP, переименовывает содержимое и возвращает новый ZIP"""
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Please upload a ZIP file")

    # Читаем входящий архив
    content = await file.read()
    input_zip = zipfile.ZipFile(io.BytesIO(content))
    
    files_data = {}
    for name in input_zip.namelist():
        if not name.endswith('/'): # Пропускаем папки
            files_data[os.path.basename(name)] = input_zip.read(name)
    
    # Обрабатываем
    processed_data = process_files(files_data)
    
    # Создаем выходящий архив
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as output_zip:
        for new_name, data in processed_data.items():
            output_zip.writestr(new_name, data)
    
    zip_buffer.seek(0)
    
    return StreamingResponse(
        zip_buffer, 
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment; filename=renamed_{file.filename}"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)