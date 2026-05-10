import os
import re
from collections import defaultdict
from typing import List, Optional, Tuple, Dict

from docx import Document
from docx.shared import Pt, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_ALIGN_VERTICAL, WD_TABLE_ALIGNMENT
from docx.oxml.ns import nsdecls, qn
from docx.oxml import parse_xml

from sqlalchemy.orm import Session, joinedload

from models import (
    IncomingDirection, Student, Specialization,
    ControlTable
)

# ══════════════════════════════════════════════
#  УТИЛИТЫ ФОРМАТИРОВАНИЯ WORD
# ══════════════════════════════════════════════

def fix_font(run):
    """Принудительно устанавливает шрифт в XML для предотвращения подмены темой Word"""
    run.font.name = 'Times New Roman'
    r = run._element.rPr.get_or_add_rFonts()
    r.set(qn('w:ascii'), 'Times New Roman')
    r.set(qn('w:hAnsi'), 'Times New Roman')
    r.set(qn('w:eastAsia'), 'Times New Roman')
    r.set(qn('w:cs'), 'Times New Roman')

def set_cell_text(
    cell,
    text: str,
    bold: bool = False,
    size: int = 10,
    alignment=WD_ALIGN_PARAGRAPH.LEFT,
):
    cell.text = ''
    paragraph = cell.paragraphs[0]
    paragraph.alignment = alignment
    paragraph.paragraph_format.space_before = Pt(0)
    paragraph.paragraph_format.space_after = Pt(0)
    run = paragraph.add_run(str(text) if text else '')
    run.font.size = Pt(size)
    run.bold = bold
    fix_font(run) # Исправление шрифта
    cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

def set_cell_shading(cell, color: str = "FFFFFF"):
    """Заливка ячейки. По умолчанию - белая (FFFFFF)"""
    shading = parse_xml(
        f'<w:shd {nsdecls("w")} w:fill="{color}"/>'
    )
    cell._tc.get_or_add_tcPr().append(shading)

def add_paragraph_text(
    doc: Document,
    text: str,
    bold: bool = False,
    size: int = 12,
    alignment=WD_ALIGN_PARAGRAPH.LEFT,
    space_after: int = 6,
    space_before: int = 0,
    first_line_indent: Optional[float] = None,
):
    para = doc.add_paragraph()
    para.alignment = alignment
    para.paragraph_format.space_after = Pt(space_after)
    para.paragraph_format.space_before = Pt(space_before)

    if first_line_indent is not None:
        para.paragraph_format.first_line_indent = Cm(first_line_indent)

    run = para.add_run(text)
    run.font.size = Pt(size)
    run.bold = bold
    fix_font(run) # Исправление шрифта
    return para

# ══════════════════════════════════════════════
#  СОЗДАНИЕ ТАБЛИЦЫ ПЕРЕАТТЕСТАЦИИ
# ══════════════════════════════════════════════

def add_control_table(doc: Document, control_rows: List[ControlTable]):
    total_cols = 7
    total_rows = 2 + len(control_rows)

    # Создаем таблицу с фиксированной шириной
    table = doc.add_table(rows=total_rows, cols=total_cols)
    table.style = 'Table Grid'
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False # Критично: отключаем авто-подбор ширины

    # Задаем ширину колонок в см (общая ширина для A4 с полями ~16.5 см)
    widths = [5.5, 1.5, 2.0, 1.5, 2.0, 1.5, 2.5]
    for i, width in enumerate(widths):
        for cell in table.columns[i].cells:
            cell.width = Cm(width)

    # ── Строка 0: Заголовки (БЕЗ СЕРОЙ ЗАЛИВКИ) ──
    
    # Наименование дисциплины
    cell_name = table.cell(0, 0)
    table.cell(0, 0).merge(table.cell(1, 0))
    set_cell_text(cell_name, 'Наименование\nдисциплины', bold=True, size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_name, "FFFFFF") # Белая заливка

    # По учебному плану
    cell_plan = table.cell(0, 1)
    table.cell(0, 1).merge(table.cell(0, 2))
    set_cell_text(cell_plan, 'По учебному плану', bold=True, size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_plan, "FFFFFF")

    # Данные приложений
    cell_diploma = table.cell(0, 3)
    table.cell(0, 3).merge(table.cell(0, 4))
    set_cell_text(cell_diploma, 'Данные приложений\nк диплому', bold=True, size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_diploma, "FFFFFF")

    # Переаттестовано
    cell_reattest = table.cell(0, 5)
    table.cell(0, 5).merge(table.cell(1, 5))
    set_cell_text(cell_reattest, 'Пере-\nаттестовано\n(часов)', bold=True, size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_reattest, "FFFFFF")

    # Форма
    cell_form = table.cell(0, 6)
    table.cell(0, 6).merge(table.cell(1, 6))
    set_cell_text(cell_form, 'Форма\nпереаттестации', bold=True, size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_form, "FFFFFF")

    # ── Строка 1: подзаголовки ──
    sub_headers = [(1, 'К-во\nчасов'), (2, 'Форма контр.'), (3, 'К-во\nчасов'), (4, 'Форма контр.')]
    for col_idx, text in sub_headers:
        cell = table.cell(1, col_idx)
        set_cell_text(cell, text, bold=True, size=8, alignment=WD_ALIGN_PARAGRAPH.CENTER)
        set_cell_shading(cell, "FFFFFF")

    # ── Строки данных ──
    for i, ct in enumerate(control_rows):
        row_idx = i + 2
        set_cell_text(table.cell(row_idx, 0), ct.study_program.name if ct.study_program else '', size=9)
        set_cell_text(table.cell(row_idx, 1), ct.hours_normal or '', size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
        set_cell_text(table.cell(row_idx, 2), (ct.format_control_norma.format_name if ct.format_control_norma else ''), size=8)
        set_cell_text(table.cell(row_idx, 3), ct.hours_fact or '', size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
        set_cell_text(table.cell(row_idx, 4), (ct.format_control_fact.format_name if ct.format_control_fact else ''), size=8)
        set_cell_text(table.cell(row_idx, 5), ct.hours_fact or '', size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)
        set_cell_text(table.cell(row_idx, 6), (ct.format_retests.format_name if ct.format_retests else ''), size=8)

    return table

# ══════════════════════════════════════════════
#  ГЕНЕРАЦИЯ ОДНОГО ДОКУМЕНТА
# ══════════════════════════════════════════════

def generate_order_for_direction(session: Session, direction: IncomingDirection, output_dir: str) -> str:
    doc = Document()

    # 1. Установка языка документа (RU) для проверки орфографии и переносов
    styles_element = doc.styles.element
    rpr_default = styles_element.xpath('./w:docDefaults/w:rPrDefault/w:rPr')[0]
    lang = parse_xml(f'<w:lang {nsdecls("w")} w:val="ru-RU" w:eastAsia="ru-RU" w:bidi="ar-SA"/>')
    rpr_default.append(lang)

    # 2. Настройка страницы (A4) и уменьшение полей
    section = doc.sections[0]
    section.page_height = Cm(29.7)
    section.page_width = Cm(21.0)
    section.left_margin = Cm(3.0)  # Поле для подшивки
    section.right_margin = Cm(1.5)
    section.top_margin = Cm(2.0)
    section.bottom_margin = Cm(2.0)

    # Базовый шрифт
    style = doc.styles['Normal']
    style.font.name = 'Times New Roman'
    style.font.size = Pt(12)

    # Данные для генерации
    students = session.query(Student).filter(Student.incoming_direction_id == direction.id).all()
    if not students: raise ValueError("Нет студентов")
    
    from word_generator import group_students_by_uni_spec, get_unique_control_data
    groups = group_students_by_uni_spec(students)
    control_data = get_unique_control_data(session, direction.id)

    # Заголовок
    add_paragraph_text(doc, 'ПРИКАЗ', bold=True, size=14, alignment=WD_ALIGN_PARAGRAPH.CENTER, space_after=12)
    add_paragraph_text(doc, 'ПРИКАЗЫВАЮ:', bold=True, size=12, space_after=6)

    # Текст приказа
    main_text = (
        f'Переаттестовать дисциплины в соответствии с учебным планом '
        f'по ускоренной образовательной программе бакалавриата по направлению подготовки {direction.name} '
        f'у нижеследующих студентов:'
    )
    add_paragraph_text(doc, main_text, size=12, first_line_indent=1.25, alignment=WD_ALIGN_PARAGRAPH.JUSTIFY)

    # Группы студентов
    for (uni_id, uni_name, spec_id, spec_name), group_students in groups.items():
        for i, student in enumerate(group_students, 1):
            add_paragraph_text(doc, f'{i}. {student.full_name}', size=12, space_after=2)

        add_paragraph_text(doc, f'прослушанных в {uni_name} по специальности «{spec_name}»:', size=12, space_before=6)
        
        # Сама таблица
        add_control_table(doc, control_data)
        add_paragraph_text(doc, '', size=6, space_after=12)

    # Сохранение
    os.makedirs(output_dir, exist_ok=True)
    safe_name = re.sub(r'[^\w\sа-яА-ЯёЁ-]', '', direction.name).replace(' ', '_')[:50]
    filepath = os.path.join(output_dir, f'Приказ_{safe_name}.docx')
    doc.save(filepath)
    return filepath