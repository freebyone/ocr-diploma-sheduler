import os
from collections import defaultdict

def get_surname(filename):
    """Извлекает фамилию из названия файла (первое слово до пробела или точки)"""
    # Убираем расширение для анализа
    name = os.path.splitext(filename)[0]
    # Если есть расширение .plx перед .xlsx, убираем и его
    if name.endswith('.plx'):
        name = name[:-4]
    
    # Берём первое слово (фамилию) - до первого пробела или подчёркивания
    surname = name.split()[0].split('_')[0]
    return surname

def rename_files_with_numbering(folder_path):
    """Переименовывает файлы, добавляя нумерацию для всех пар"""
    
    # Получаем все файлы в папке
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    
    # Группируем файлы по фамилии
    surname_files = defaultdict(list)
    for filename in files:
        surname = get_surname(filename)
        surname_files[surname].append(filename)
    
    # Счётчик для нумерации пар
    pair_number = 1
    
    # Сортируем фамилии для предсказуемого порядка
    for surname in sorted(surname_files.keys()):
        file_list = surname_files[surname]
        
        # Разделяем на xlsx и pdf
        xlsx_files = sorted([f for f in file_list if f.endswith('.xlsx')])
        pdf_files = sorted([f for f in file_list if f.endswith('.pdf')])
        
        # Определяем сколько пар (берём максимум из xlsx и pdf)
        num_pairs = max(len(xlsx_files), len(pdf_files))
        
        print(f"\nФамилия '{surname}': {len(xlsx_files)} xlsx, {len(pdf_files)} pdf")
        
        # Обрабатываем каждую пару
        for i in range(num_pairs):
            prefix = f"{pair_number:03d}_"
            
            # Переименовываем xlsx если есть
            if i < len(xlsx_files):
                filename = xlsx_files[i]
                new_name = prefix + filename
                old_path = os.path.join(folder_path, filename)
                new_path = os.path.join(folder_path, new_name)
                print(f"  {filename} -> {new_name}")
                os.rename(old_path, new_path)
            
            # Переименовываем pdf если есть
            if i < len(pdf_files):
                filename = pdf_files[i]
                new_name = prefix + filename
                old_path = os.path.join(folder_path, filename)
                new_path = os.path.join(folder_path, new_name)
                print(f"  {filename} -> {new_name}")
                os.rename(old_path, new_path)
            
            pair_number += 1

def main():
    # Путь к папке с файлами
    folder_path = "./dataset/check"
    
    # Проверяем существование папки
    if not os.path.exists(folder_path):
        print(f"Папка '{folder_path}' не найдена!")
        return
    
    print("=== Начало переименования ===")
    rename_files_with_numbering(folder_path)
    print("\n=== Готово! ===")

if __name__ == "__main__":
    main()