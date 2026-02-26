"""
CRUD операции для всех сущностей БД.
"""

from sqlalchemy.orm import Session
from sqlalchemy import select
from models import (
    Direction, University, Specialization, Student,
    FormatControl, FormatRetests, StudyProgram, ControlTable
)
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


# ==================== Direction ====================

def get_direction_by_name(session: Session, name: str) -> Optional[Direction]:
    stmt = select(Direction).where(Direction.name == name)
    return session.execute(stmt).scalar_one_or_none()


def get_direction_by_id(session: Session, direction_id: int) -> Optional[Direction]:
    return session.get(Direction, direction_id)


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


def delete_direction(session: Session, direction_id: int) -> bool:
    direction = session.get(Direction, direction_id)
    if direction:
        session.delete(direction)
        session.flush()
        return True
    return False


# ==================== University ====================

def get_university_by_name(session: Session, name: str) -> Optional[University]:
    stmt = select(University).where(University.name == name)
    return session.execute(stmt).scalar_one_or_none()


def get_university_by_id(session: Session, university_id: int) -> Optional[University]:
    return session.get(University, university_id)


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


def delete_university(session: Session, university_id: int) -> bool:
    university = session.get(University, university_id)
    if university:
        session.delete(university)
        session.flush()
        return True
    return False


# ==================== Specialization ====================

def get_specialization_by_name(
    session: Session, name: str
) -> Optional[Specialization]:
    stmt = select(Specialization).where(Specialization.name == name)
    return session.execute(stmt).scalar_one_or_none()


def get_specialization_by_id(
    session: Session, spec_id: int
) -> Optional[Specialization]:
    return session.get(Specialization, spec_id)


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
    session: Session, name: str, direction_id: int, university_id: int
) -> Specialization:
    spec = get_specialization_full(session, name, direction_id, university_id)
    if spec:
        logger.info(f"Found existing specialization: {spec}")
        return spec

    spec = Specialization(
        name=name,
        direction_id=direction_id,
        university_id=university_id
    )
    session.add(spec)
    session.flush()
    logger.info(f"Created new specialization: {spec}")
    return spec


def get_all_specializations(session: Session) -> List[Specialization]:
    stmt = select(Specialization).order_by(Specialization.id)
    return list(session.execute(stmt).scalars().all())


# ==================== Student ====================

def get_student_by_name(session: Session, full_name: str) -> Optional[Student]:
    stmt = select(Student).where(Student.full_name == full_name)
    return session.execute(stmt).scalar_one_or_none()


def get_student_by_id(session: Session, student_id: int) -> Optional[Student]:
    return session.get(Student, student_id)


def get_student_by_name_and_spec(
    session: Session, full_name: str, specialization_id: int
) -> Optional[Student]:
    stmt = select(Student).where(
        Student.full_name == full_name,
        Student.specialization_id == specialization_id
    )
    return session.execute(stmt).scalar_one_or_none()


def get_or_create_student(
    session: Session, full_name: str, specialization_id: int
) -> Student:
    student = get_student_by_name_and_spec(
        session, full_name, specialization_id
    )
    if student:
        logger.info(f"Found existing student: {student}")
        return student

    student = Student(
        full_name=full_name,
        specialization_id=specialization_id
    )
    session.add(student)
    session.flush()
    logger.info(f"Created new student: {student}")
    return student


def get_all_students(session: Session) -> List[Student]:
    stmt = select(Student).order_by(Student.id)
    return list(session.execute(stmt).scalars().all())


def get_students_by_specialization(
    session: Session, specialization_id: int
) -> List[Student]:
    stmt = select(Student).where(
        Student.specialization_id == specialization_id
    ).order_by(Student.id)
    return list(session.execute(stmt).scalars().all())


def delete_student(session: Session, student_id: int) -> bool:
    student = session.get(Student, student_id)
    if student:
        session.delete(student)
        session.flush()
        return True
    return False


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
    specialization_id: int,
    study_program_id: int,
    format_control_norma_id: int,
    format_control_fact_id: int,
    format_retests_id: int,
    hours_fact: str,
    hours_normal: str
) -> ControlTable:
    entry = ControlTable(
        specialization_id=specialization_id,
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


def get_control_table_by_spec(
    session: Session, specialization_id: int
) -> List[ControlTable]:
    stmt = select(ControlTable).where(
        ControlTable.specialization_id == specialization_id
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
    specialization_name: str
) -> dict:
    """
    Сохраняет данные диплома в БД.
    Создаёт или находит все связанные сущности.
    """
    logger.info("=" * 50)
    logger.info("Saving diploma data to database")
    logger.info(f"  FIO:            {full_name}")
    logger.info(f"  Direction:      {direction_name}")
    logger.info(f"  University:     {university_name}")
    logger.info(f"  Specialization: {specialization_name}")

    # 1. Направление
    direction = get_or_create_direction(session, direction_name)

    # 2. Учебное заведение
    university = get_or_create_university(session, university_name)

    # 3. Специализация
    specialization = get_or_create_specialization(
        session, specialization_name, direction.id, university.id
    )

    # 4. Студент
    student = get_or_create_student(
        session, full_name, specialization.id
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
        "student": student
    }