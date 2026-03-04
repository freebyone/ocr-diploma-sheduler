import re
from typing import Optional, Dict, List, Tuple, Any
import pandas as pd
# from sqlalchemy import create_engine, select
# from sqlalchemy.orm import Session, sessionmaker
import logging

# Исправленные импорты для работы в структуре src
# from src.models import (
#     Base, Direction, Specialization, 
#     StudyProgram, FormatControl, FormatRetests, ControlTable
# )

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XLSXParser:
    def __init__(self, file_path: str, db_url: str = None, sheets: List[str] = ["Титул", "Переаттестация"]):
        self.file_path = file_path
        self.wb = pd.read_excel(file_path, sheet_name=sheets)
        
        # Настройка подключения к БД
        # self.engine = create_engine(db_url)
        # self.Session = sessionmaker(bind=self.engine)

    def start_parse(self):
        # print(type(self.wb['Титул']))
        # print(self.wb['Переаттестация'])
        df = self.wb['Переаттестация']
        # ──────────────────────────────────────────────
        # 1. Извлекаем верхний уровень и подуровень
        # ──────────────────────────────────────────────
        top_headers = df.columns.tolist()
        sub_headers = df.iloc[1].tolist()   # строка с реальными именами колонок

        # ──────────────────────────────────────────────
        # 2. Определяем принадлежность к группе
        # ──────────────────────────────────────────────
        def build_group_mapping(top_headers):
            """
            '-' / '-.N'    → одиночная колонка (group = None)
            'Unnamed: N'   → часть предыдущей объединённой ячейки
            всё остальное  → новое имя группы
            """
            groups = []
            current_group = None

            for col in top_headers:
                col_str = str(col)

                if col_str.startswith('Unnamed'):
                    # Продолжение merged-ячейки → та же группа
                    groups.append(current_group)

                elif col_str == '-' or col_str.startswith('-.'):
                    # Одиночная колонка
                    groups.append(None)
                    current_group = None

                else:
                    # Новая группа
                    current_group = col_str
                    groups.append(current_group)

            return groups

        groups = build_group_mapping(top_headers)
        # [None, None, None, 'По плану', 'По плану', None,
        #  'Изучено и зачтено', 'Изучено и зачтено', None, None, None]

        # ──────────────────────────────────────────────
        # 3. Вырезаем только данные (без заголовков и итогов)
        # ──────────────────────────────────────────────
        data = df.iloc[2:].reset_index(drop=True)

        # Убираем строку "Итого" и полностью пустые
        data = data[data.iloc[:, 1].notna()]                    # где есть Наименование
        data = data[~data.iloc[:, 1].str.contains('Итого', na=False)]
        data = data.reset_index(drop=True)

        # ──────────────────────────────────────────────
        # 4. Собираем структурированный словарь
        # ──────────────────────────────────────────────
        result = {}

        for i, (group, sub) in enumerate(zip(groups, sub_headers)):
            col_data = data.iloc[:, i].tolist()

            if group:
                # Сгруппированная колонка → вложенный словарь
                result.setdefault(group, {})[sub] = col_data
            else:
                # Одиночная колонка → верхний уровень
                if pd.notna(sub):
                    result[sub] = col_data

        print(result.keys())
        # print(result)
        for k,v in result.items():
            print(k,v)

        import json
        with open('./r.json','w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
        # col = wb.columns
        # for c in col:
        #     if not c:
        #     print(f"\nCOL: {c}\n")
        #     print(self.wb['Переаттестация'][c])

        


