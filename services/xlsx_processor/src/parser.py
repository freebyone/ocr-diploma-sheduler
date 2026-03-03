import pandas as pd
import json
from sqlalchemy import create_engine, text

engine = create_engine('postgresql://user:pass@localhost/db')


def parse_excel_structure(filepath):
    """
    Парсит Excel и возвращает:
    - column_structure: описание колонок для воссоздания таблицы
    - rows: список словарей с данными
    """
    df = pd.read_excel(filepath, engine='openpyxl')

    top_headers = df.columns.tolist()
    sub_headers = df.iloc[1].tolist()

    # ── Формируем структуру колонок ──
    column_structure = []
    current_group = None

    for i, (top, sub) in enumerate(zip(top_headers, sub_headers)):
        top_str = str(top)
        sub_str = str(sub) if pd.notna(sub) else None

        if top_str.startswith('Unnamed'):
            group = current_group
        elif top_str == '-' or top_str.startswith('-.'):
            group = None
            current_group = None
        else:
            group = top_str
            current_group = top_str

        # Уникальный ключ для хранения в row_data
        if group and sub_str:
            flat_key = f"{group}__{sub_str}"
        elif sub_str:
            flat_key = sub_str
        else:
            flat_key = f"col_{i}"

        column_structure.append({
            "index": i,
            "group": group,          # верхний уровень ("По плану")
            "name": sub_str,         # подзаголовок ("з.е.")
            "flat_key": flat_key     # ключ в row_data
        })

    # ── Формируем строки данных ──
    data_df = df.iloc[2:].reset_index(drop=True)

    # Фильтруем пустые и "Итого"
    data_df = data_df[data_df.iloc[:, 1].notna()]
    data_df = data_df[
        ~data_df.iloc[:, 1].astype(str).str.contains('Итого', na=False)
    ]
    data_df = data_df.reset_index(drop=True)

    rows = []
    for _, row in data_df.iterrows():
        row_dict = {}
        for col_info in column_structure:
            val = row.iloc[col_info["index"]]
            # Приводим к JSON-совместимому типу
            if pd.isna(val):
                val = None
            elif hasattr(val, 'item'):  # numpy int/float
                val = val.item()
            row_dict[col_info["flat_key"]] = val
        rows.append(row_dict)

    return column_structure, rows


def save_to_db(filepath, template_name, student_id=None):
    """Сохраняет в PostgreSQL"""
    column_structure, rows = parse_excel_structure(filepath)

    with engine.begin() as conn:
        # Сохраняем шаблон
        result = conn.execute(
            text("""
                INSERT INTO table_templates (name, column_structure)
                VALUES (:name, :structure)
                RETURNING id
            """),
            {
                "name": template_name,
                "structure": json.dumps(
                    column_structure, ensure_ascii=False
                )
            }
        )
        template_id = result.fetchone()[0]

        # Сохраняем строки
        for i, row in enumerate(rows):
            conn.execute(
                text("""
                    INSERT INTO table_data
                        (template_id, student_id, row_index, row_data)
                    VALUES (:tid, :sid, :idx, :data)
                """),
                {
                    "tid": template_id,
                    "sid": student_id,
                    "idx": i,
                    "data": json.dumps(row, ensure_ascii=False)
                }
            )

    return template_id


# Использование
template_id = save_to_db(
    'inp.xlsx',
    'Индивидуальный учебный план',
    student_id=42
)