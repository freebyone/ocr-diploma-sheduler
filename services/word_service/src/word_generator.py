import os
import re
from collections import defaultdict
from typing import List, Optional, Tuple, Dict

from docx import Document
from docx.shared import Pt, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.oxml.ns import nsdecls
from docx.oxml import parse_xml

from sqlalchemy.orm import Session, joinedload

from models import (
    IncomingDirection, Student, Specialization,
    ControlTable, StudyProgram, FormatControl, FormatRetests,
)


# ══════════════════════════════════════════════
#  УТИЛИТЫ ФОРМАТИРОВАНИЯ WORD
# ══════════════════════════════════════════════

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
    run.font.name = 'Times New Roman'
    run.bold = bold
    cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER


def set_cell_shading(cell, color: str = "F2F2F2"):
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
    run.font.name = 'Times New Roman'
    run.bold = bold
    return para


# ══════════════════════════════════════════════
#  СОЗДАНИЕ ТАБЛИЦЫ ПЕРЕАТТЕСТАЦИИ
# ══════════════════════════════════════════════

def add_control_table(doc: Document, control_rows: List[ControlTable]):
    total_cols = 7
    total_rows = 2 + len(control_rows)

    table = doc.add_table(rows=total_rows, cols=total_cols)
    table.style = 'Table Grid'

    # ── Строка 0: верхний уровень заголовков ──

    # Наименование дисциплины (rowspan=2)
    cell_name = table.cell(0, 0)
    table.cell(0, 0).merge(table.cell(1, 0))
    set_cell_text(cell_name, 'Наименование\nдисциплины', bold=True, size=9,
                  alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_name)

    # По учебному плану (colspan=2)
    cell_plan = table.cell(0, 1)
    table.cell(0, 1).merge(table.cell(0, 2))
    set_cell_text(cell_plan, 'По учебному плану', bold=True, size=9,
                  alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_plan)

    # Данные приложений к диплому (colspan=2)
    cell_diploma = table.cell(0, 3)
    table.cell(0, 3).merge(table.cell(0, 4))
    set_cell_text(cell_diploma, 'Данные приложений\nк диплому', bold=True, size=9,
                  alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_diploma)

    # Переаттестовано (часов) (rowspan=2)
    cell_reattest = table.cell(0, 5)
    table.cell(0, 5).merge(table.cell(1, 5))
    set_cell_text(cell_reattest, 'Пере-\nаттестовано\n(часов)', bold=True, size=9,
                  alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_reattest)

    # Форма переаттестации (rowspan=2)
    cell_form = table.cell(0, 6)
    table.cell(0, 6).merge(table.cell(1, 6))
    set_cell_text(cell_form, 'Форма\nпереаттестации', bold=True, size=9,
                  alignment=WD_ALIGN_PARAGRAPH.CENTER)
    set_cell_shading(cell_form)

    # ── Строка 1: подзаголовки ──
    sub_headers = [
        (1, 'К-во\nчасов'),
        (2, 'Форма итогового\nконтроля'),
        (3, 'К-во\nчасов'),
        (4, 'Форма итогового\nконтроля'),
    ]
    for col_idx, text in sub_headers:
        cell = table.cell(1, col_idx)
        set_cell_text(cell, text, bold=True, size=8,
                      alignment=WD_ALIGN_PARAGRAPH.CENTER)
        set_cell_shading(cell, "E6E6E6")

    # ── Строки данных ──
    for i, ct in enumerate(control_rows):
        row_idx = i + 2

        prog_name = ct.study_program.name if ct.study_program else ''
        set_cell_text(table.cell(row_idx, 0), prog_name, size=9)

        set_cell_text(
            table.cell(row_idx, 1),
            ct.hours_normal or '',
            size=9,
            alignment=WD_ALIGN_PARAGRAPH.CENTER
        )

        fc_norma_name = (
            ct.format_control_norma.format_name
            if ct.format_control_norma else ''
        )
        set_cell_text(table.cell(row_idx, 2), fc_norma_name, size=9)

        set_cell_text(
            table.cell(row_idx, 3),
            ct.hours_fact or '',
            size=9,
            alignment=WD_ALIGN_PARAGRAPH.CENTER
        )

        fc_fact_name = (
            ct.format_control_fact.format_name
            if ct.format_control_fact else ''
        )
        set_cell_text(table.cell(row_idx, 4), fc_fact_name, size=9)

        set_cell_text(
            table.cell(row_idx, 5),
            ct.hours_fact or '',
            size=9,
            alignment=WD_ALIGN_PARAGRAPH.CENTER
        )

        fr_name = (
            ct.format_retests.format_name
            if ct.format_retests else ''
        )
        set_cell_text(table.cell(row_idx, 6), fr_name, size=9)

    return table


# ══════════════════════════════════════════════
#  ГРУППИРОВКА СТУДЕНТОВ
# ══════════════════════════════════════════════

GroupKey = Tuple[int, str, int, str]


def group_students_by_uni_spec(
    students: List[Student],
) -> Dict[GroupKey, List[Student]]:
    groups: Dict[GroupKey, List[Student]] = defaultdict(list)

    for student in students:
        spec = student.specialization
        if spec and spec.university:
            key: GroupKey = (
                spec.university.id,
                spec.university.name,
                spec.id,
                spec.name,
            )
        elif spec:
            key = (0, 'Учебное заведение не указано', spec.id, spec.name)
        else:
            key = (0, 'Учебное заведение не указано', 0, 'Специальность не указана')

        groups[key].append(student)

    for key in groups:
        groups[key].sort(key=lambda s: s.full_name)

    return dict(groups)


def get_unique_control_data(
    session: Session,
    direction_id: int,
) -> List[ControlTable]:
    all_rows = (
        session.query(ControlTable)
        .filter(ControlTable.incoming_direction_id == direction_id)
        .options(
            joinedload(ControlTable.study_program),
            joinedload(ControlTable.format_control_norma),
            joinedload(ControlTable.format_control_fact),
            joinedload(ControlTable.format_retests),
        )
        .all()
    )

    seen = set()
    unique = []
    for ct in all_rows:
        prog_id = ct.study_program_id
        if prog_id not in seen:
            seen.add(prog_id)
            unique.append(ct)

    return unique


# ══════════════════════════════════════════════
#  ГЕНЕРАЦИЯ ОДНОГО ДОКУМЕНТА
# ══════════════════════════════════════════════

def generate_order_for_direction(
    session: Session,
    direction: IncomingDirection,
    output_dir: str,
) -> str:
    """Генерирует Word-приказ для одного направления, возвращает путь к файлу."""

    # ── Студенты ──
    students = (
        session.query(Student)
        .filter(Student.incoming_direction_id == direction.id)
        .options(
            joinedload(Student.specialization)
            .joinedload(Specialization.university)
        )
        .all()
    )

    if not students:
        raise ValueError(f"Нет студентов для направления id={direction.id}")

    # ── Группировка ──
    groups = group_students_by_uni_spec(students)

    # ── Данные таблицы ──
    control_data = get_unique_control_data(session, direction.id)

    if not control_data:
        raise ValueError(
            f"Нет данных ControlTable для направления id={direction.id}"
        )

    # ══════════════════════════════════════════
    #  ФОРМИРУЕМ ДОКУМЕНТ
    # ══════════════════════════════════════════
    doc = Document()

    style = doc.styles['Normal']
    font = style.font
    font.name = 'Times New Roman'
    font.size = Pt(12)

    # ── Заголовок ──
    add_paragraph_text(
        doc, 'ПРИКАЗ',
        bold=True, size=14,
        alignment=WD_ALIGN_PARAGRAPH.CENTER,
        space_after=12,
    )

    # ── ПРИКАЗЫВАЮ ──
    add_paragraph_text(
        doc, 'ПРИКАЗЫВАЮ:',
        bold=True, size=12,
        space_after=6,
    )

    # ── Основной текст ──
    direction_name = direction.name
    main_text = (
        f'Переаттестовать дисциплины в соответствии с учебным планом '
        f'по ускоренной образовательной программе бакалавриата '
        f'(на базе профильного среднего профессионального образования) '
        f'по направлению подготовки {direction_name} '
        f'у нижеследующих студентов 1 курса, очно-заочной (вечерней) '
        f'формы обучения факультета '
        f'«Плехановская школа бизнеса «Интеграл»:'
    )
    add_paragraph_text(
        doc, main_text,
        size=12,
        first_line_indent=1.25,
        space_after=12,
    )

    # ── Группы студентов ──
    for (uni_id, uni_name, spec_id, spec_name), group_students in groups.items():
        for i, student in enumerate(group_students, 1):
            add_paragraph_text(
                doc,
                f'{i}. {student.full_name}',
                size=12,
                space_after=2,
                space_before=0,
            )

        add_paragraph_text(
            doc,
            f'прослушанных в {uni_name} '
            f'по специальности «{spec_name}»:',
            size=12,
            space_before=6,
            space_after=6,
        )

        add_control_table(doc, control_data)

        add_paragraph_text(doc, '', size=6, space_after=12)

    # ── Сохранение ──
    os.makedirs(output_dir, exist_ok=True)

    safe_name = re.sub(r'[^\w\sа-яА-ЯёЁ-]', '', direction.name)
    safe_name = safe_name[:80].strip().replace(' ', '_')
    filename = f'Приказ_{safe_name}.docx'
    filepath = os.path.join(output_dir, filename)

    doc.save(filepath)
    return filepath