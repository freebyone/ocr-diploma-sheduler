"""
CRUD операции для всех сущностей БД.
Адаптировано под новую схему с IncomingDirection, ExcelDataFile.
"""

from sqlalchemy.orm import Session
from sqlalchemy import select
from models import (
    Direction, University, Specialization, Student,
    FormatControl, FormatRetests, StudyProgram, ControlTable,
    IncomingDirection, ExcelDataFile
)
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


# ==================== Direction ====================

def get_direction_by_name(session: Session, name: str) -> Optional[Direction]:
    stmt = select(Direction).where(Direction.name == name)
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_direction(session: Session, name: str) -> Direction:
    direction = get_direction_by_name(session, name)
    if direction:
        logger.info(f"Found existing direction: {direction}")
        return direction

    direction = Direction(name=name)
    session.add(direction)
    session.flush()
    logger.info(f"Created new direction: {direction}")
    return direction


def get_all_directions(session: Session) -> List[Direction]:
    stmt = select(Direction).order_by(Direction.id)
    return list(session.execute(stmt).scalars().all())


# ==================== University ====================

def get_university_by_name(
    session: Session, name: str
) -> Optional[University]:
    stmt = select(University).where(University.name == name)
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_university(session: Session, name: str) -> University:
    university = get_university_by_name(session, name)
    if university:
        logger.info(f"Found existing university: {university}")
        return university

    university = University(name=name)
    session.add(university)
    session.flush()
    logger.info(f"Created new university: {university}")
    return university


def get_all_universities(session: Session) -> List[University]:
    stmt = select(University).order_by(University.id)
    return list(session.execute(stmt).scalars().all())


# ==================== Specialization ====================

def get_specialization_full(
    session: Session, name: str, direction_id: int, university_id: int
) -> Optional[Specialization]:
    stmt = select(Specialization).where(
        Specialization.name == name,
        Specialization.direction_id == direction_id,
        Specialization.university_id == university_id
    )
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_specialization(
    session: Session, name: str, direction_id: int,
    university_id: int, specialization_code: str
) -> Specialization:
    spec = get_specialization_full(session, name, direction_id, university_id)
    if spec:
        logger.info(f"Found existing specialization: {spec}")
        return spec

    spec = Specialization(
        name=name,
        direction_id=direction_id,
        university_id=university_id,
        code=specialization_code
    )
    session.add(spec)
    session.flush()
    logger.info(f"Created new specialization: {spec}")
    return spec


def get_all_specializations(session: Session) -> List[Specialization]:
    stmt = select(Specialization).order_by(Specialization.id)
    return list(session.execute(stmt).scalars().all())


# ==================== IncomingDirection ====================

def get_incoming_direction_by_name(
    session: Session, name: str
) -> Optional[IncomingDirection]:
    stmt = select(IncomingDirection).where(IncomingDirection.name == name)
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_incoming_direction(
    session: Session, name: str
) -> IncomingDirection:
    inc_dir = get_incoming_direction_by_name(session, name)
    if inc_dir:
        logger.info(f"Found existing incoming_direction: id={inc_dir.id}")
        return inc_dir

    inc_dir = IncomingDirection(name=name)
    session.add(inc_dir)
    session.flush()
    logger.info(f"Created new incoming_direction: id={inc_dir.id}")
    return inc_dir


# ==================== ExcelDataFile ====================

def get_excel_file_by_code(
    session: Session, code_file: str
) -> Optional[ExcelDataFile]:
    stmt = select(ExcelDataFile).where(ExcelDataFile.code_file == code_file)
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_excel_file(
    session: Session, name: str, full_name: str,
    code_file: str, incoming_direction_id: int
) -> ExcelDataFile:
    excel_file = get_excel_file_by_code(session, code_file)
    if excel_file:
        logger.info(f"Found existing excel_data_file: id={excel_file.id}")
        return excel_file

    excel_file = ExcelDataFile(
        name=name,
        full_name=full_name,
        code_file=code_file,
        incoming_direction_id=incoming_direction_id
    )
    session.add(excel_file)
    session.flush()
    logger.info(f"Created new excel_data_file: id={excel_file.id}")
    return excel_file


# ==================== Student ====================

def get_student_by_file_code(
    session: Session, file_code: str
) -> Optional[Student]:
    """Ищет студента по file_code (префиксу из имени файла)."""
    stmt = select(Student).where(Student.file_code == file_code)
    return session.execute(stmt).scalar_one_or_none()


def get_student_by_name(
    session: Session, full_name: str
) -> Optional[Student]:
    """Ищет студента только по ФИО."""
    stmt = select(Student).where(Student.full_name == full_name)
    return session.execute(stmt).scalar_one_or_none()


def get_student_by_name_and_spec(
    session: Session, full_name: str, specialization_id: int
) -> Optional[Student]:
    """Ищет студента по ФИО + специализации."""
    stmt = select(Student).where(
        Student.full_name == full_name,
        Student.specialization_id == specialization_id
    )
    return session.execute(stmt).scalar_one_or_none()


def student_exists(
    session: Session, full_name: str, specialization_id: int, file_code: str
) -> Optional[Student]:
    """
    Проверяет существует ли студент по любому из критериев:
    1. По file_code (уникальный префикс файла)
    2. По ФИО (мог быть создан Excel-сервисом без specialization)
    3. По ФИО + специализации

    Возвращает найденного студента или None.
    """
    # Сначала по file_code — самый точный критерий
    existing = get_student_by_file_code(session, file_code)
    if existing:
        return existing

    # Затем по ФИО (мог быть создан Excel-сервисом)
    existing = get_student_by_name(session, full_name)
    if existing:
        return existing

    # По ФИО + специализации
    existing = get_student_by_name_and_spec(
        session, full_name, specialization_id
    )
    if existing:
        return existing

    return None


def create_student(
    session: Session, full_name: str, specialization_id: int,
    file_code: str, file_name: str = None,
    incoming_direction_id: int = None
) -> Student:
    """Создаёт нового студента."""
    student = Student(
        full_name=full_name,
        specialization_id=specialization_id,
        file_code=file_code,
        file_name=file_name,
        incoming_direction_id=incoming_direction_id
    )
    session.add(student)
    session.flush()
    logger.info(f"Created new student: {student}")
    return student


def get_all_students(session: Session) -> List[Student]:
    stmt = select(Student).order_by(Student.id)
    return list(session.execute(stmt).scalars().all())


# ==================== FormatControl ====================

def get_format_control_by_name(
    session: Session, format_name: str
) -> Optional[FormatControl]:
    stmt = select(FormatControl).where(
        FormatControl.format_name == format_name
    )
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_format_control(
    session: Session, format_name: str
) -> FormatControl:
    fc = get_format_control_by_name(session, format_name)
    if fc:
        return fc

    fc = FormatControl(format_name=format_name)
    session.add(fc)
    session.flush()
    logger.info(f"Created new format control: {fc}")
    return fc


def get_all_format_controls(session: Session) -> List[FormatControl]:
    stmt = select(FormatControl).order_by(FormatControl.id)
    return list(session.execute(stmt).scalars().all())


# ==================== FormatRetests ====================

def get_format_retests_by_name(
    session: Session, format_name: str
) -> Optional[FormatRetests]:
    stmt = select(FormatRetests).where(
        FormatRetests.format_name == format_name
    )
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_format_retests(
    session: Session, format_name: str
) -> FormatRetests:
    fr = get_format_retests_by_name(session, format_name)
    if fr:
        return fr

    fr = FormatRetests(format_name=format_name)
    session.add(fr)
    session.flush()
    logger.info(f"Created new format retests: {fr}")
    return fr


# ==================== StudyProgram ====================

def get_study_program_by_name(
    session: Session, name: str
) -> Optional[StudyProgram]:
    stmt = select(StudyProgram).where(StudyProgram.name == name)
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_study_program(
    session: Session, name: str
) -> StudyProgram:
    sp = get_study_program_by_name(session, name)
    if sp:
        return sp

    sp = StudyProgram(name=name)
    session.add(sp)
    session.flush()
    logger.info(f"Created new study program: {sp}")
    return sp


def get_all_study_programs(session: Session) -> List[StudyProgram]:
    stmt = select(StudyProgram).order_by(StudyProgram.id)
    return list(session.execute(stmt).scalars().all())


# ==================== ControlTable ====================

def create_control_table_entry(
    session: Session,
    incoming_direction_id: int,
    study_program_id: int = None,
    format_control_norma_id: int = None,
    format_control_fact_id: int = None,
    format_retests_id: int = None,
    hours_fact: str = None,
    hours_normal: str = None
) -> ControlTable:
    entry = ControlTable(
        incoming_direction_id=incoming_direction_id,
        study_program_id=study_program_id,
        format_control_norma_id=format_control_norma_id,
        format_control_fact_id=format_control_fact_id,
        format_retests_id=format_retests_id,
        hours_fact=hours_fact,
        hours_normal=hours_normal
    )
    session.add(entry)
    session.flush()
    logger.info(f"Created control table entry: id={entry.id}")
    return entry


def get_control_table_by_direction(
    session: Session, incoming_direction_id: int
) -> List[ControlTable]:
    stmt = select(ControlTable).where(
        ControlTable.incoming_direction_id == incoming_direction_id
    ).order_by(ControlTable.id)
    return list(session.execute(stmt).scalars().all())


def get_all_control_table_entries(session: Session) -> List[ControlTable]:
    stmt = select(ControlTable).order_by(ControlTable.id)
    return list(session.execute(stmt).scalars().all())


# ==================== Комплексная операция ====================

def save_diploma_data(
    session: Session,
    full_name: str,
    direction_name: str,
    university_name: str,
    specialization_name: str,
    specialization_code: str,
    file_code: str,
    file_name: str = None
) -> dict:
    """
    Сохраняет данные диплома в БД.

    Логика поиска студента:
    1. Ищем по file_code
    2. Ищем по ФИО (мог быть создан Excel-сервисом)
    3. Ищем по ФИО + specialization_id

    Если студент найден и у него specialization_id = None,
    дозаполняем specialization_id из OCR-данных.

    Если студент не найден — создаём нового.
    """
    logger.info("=" * 50)
    logger.info("Saving diploma data to database")
    logger.info(f"  FIO:            {full_name}")
    logger.info(f"  Direction:      {direction_name}")
    logger.info(f"  University:     {university_name}")
    logger.info(f"  Specialization: {specialization_name}")
    logger.info(f"  Spec CODE:      {specialization_code}")
    logger.info(f"  File CODE:      {file_code}")
    logger.info(f"  File NAME:      {file_name}")

    # 1. Направление (квалификация)
    direction = get_or_create_direction(session, direction_name)

    # 2. Учебное заведение
    university = get_or_create_university(session, university_name)

    # 3. Специализация
    specialization = get_or_create_specialization(
        session, specialization_name, direction.id,
        university.id, specialization_code
    )

    # 4. Проверяем — есть ли уже такой студент?
    existing_student = student_exists(
        session, full_name, specialization.id, file_code
    )

    if existing_student:
        logger.info(
            f"⚠️ Student already exists: id={existing_student.id}, "
            f"name='{existing_student.full_name}', "
            f"file_code='{existing_student.file_code}', "
            f"specialization_id={existing_student.specialization_id}"
        )

        # Дозаполняем specialization_id если пустой
        # (студент мог быть создан Excel-сервисом без specialization)
        updated_fields = []

        if existing_student.specialization_id is None:
            existing_student.specialization_id = specialization.id
            updated_fields.append(
                f"specialization_id → {specialization.id}"
            )

        # Дозаполняем file_code если пустой
        # (студент мог быть создан Excel-сервисом)
        if existing_student.file_code is None and file_code:
            existing_student.file_code = file_code
            updated_fields.append(f"file_code → {file_code}")

        # Дозаполняем file_name если пустой
        if existing_student.file_name is None and file_name:
            existing_student.file_name = file_name
            updated_fields.append(f"file_name → {file_name}")

        if updated_fields:
            session.flush()
            logger.info(
                f"✏️ Updated existing student: "
                f"{', '.join(updated_fields)}"
            )
        else:
            logger.info("Skipping student creation (duplicate, all fields filled)")

        return {
            "direction": direction,
            "university": university,
            "specialization": specialization,
            "student": existing_student,
            "is_new_student": False
        }

    # 5. Создаём нового студента
    #    incoming_direction_id = None — заполнится из Excel-сервиса
    student = create_student(
        session, full_name, specialization.id,
        file_code=file_code,
        file_name=file_name,
        incoming_direction_id=None
    )

    logger.info("Diploma data saved successfully")
    logger.info(f"  Direction ID:      {direction.id}")
    logger.info(f"  University ID:     {university.id}")
    logger.info(f"  Specialization ID: {specialization.id}")
    logger.info(f"  Student ID:        {student.id}")

    return {
        "direction": direction,
        "university": university,
        "specialization": specialization,
        "student": student,
        "is_new_student": True
    }