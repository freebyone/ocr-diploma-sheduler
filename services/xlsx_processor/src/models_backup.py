from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import (
    Mapped, mapped_column, relationship, DeclarativeBase
)
from typing import List, Optional


class Base(DeclarativeBase):
    pass


class Direction(Base):
    __tablename__ = 'directions'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(200), nullable=False, comment='Квалификация'
    )

    specializations: Mapped[List["Specialization"]] = relationship(
        back_populates="direction"
    )

    def __repr__(self) -> str:
        return f"<Direction(id={self.id}, name='{self.name}')>"


class University(Base):
    __tablename__ = 'university'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(200), nullable=False, comment='Наименование учебного заведения'
    )

    specializations: Mapped[List["Specialization"]] = relationship(
        back_populates="university"
    )

    def __repr__(self) -> str:
        return f"<University(id={self.id}, name='{self.name}')>"


class Specialization(Base):
    __tablename__ = 'specialization'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(200), nullable=False, comment='Наименование специальности'
    )
    code: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True, default='', comment='Код специальности'
    )
    direction_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey('directions.id'), nullable=True, comment='ID Направления'
    )
    university_id: Mapped[Optional[int]] = mapped_column(  # Изменено на Optional
        ForeignKey('university.id'), nullable=True,  # nullable=True
        comment='ID Учебного заведения'
    )

    direction: Mapped[Optional["Direction"]] = relationship(
        back_populates="specializations"
    )
    university: Mapped[Optional["University"]] = relationship(
        back_populates="specializations"
    )
    students: Mapped[List["Student"]] = relationship(
        back_populates="specialization"
    )
    # control_tables: Mapped[List["ControlTable"]] = relationship(
    #     back_populates="specialization"
    # )

    def __repr__(self) -> str:
        return f"<Specialization(id={self.id}, name='{self.name}')>"

class IncomingDirection(Base):
    __tablename__ = 'incoming_direction'

    id: Mapped[int] =  mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(300), nullable=False, comment='Название направления из эксель с титула'
    )
    incoming_direction_files: Mapped[List["ExcelDataFile"]] = relationship(
        back_populates="excel_rel"
    )
    incoming_direction_student: Mapped[List["Student"]] = relationship(
        back_populates="inc_student"
    )
    incoming_direction_table: Mapped[List["ControlTable"]] = relationship(
        back_populates="inc_cntrl_table"
    )

class ExcelDataFile(Base):
    __tablename__ = 'excel_data_file'

    id: Mapped[int] =  mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(300), nullable=False, comment='Название файла эксель'
    )
    full_name: Mapped[str] = mapped_column(
        String(300), nullable=False, comment='ФИО из эксель с титула'
    )
    code_file: Mapped[str] = mapped_column(nullable=False, unique=True)
    incoming_direction_id: Mapped[int] = mapped_column(ForeignKey('incoming_direction.id'), nullable=False)

    # incoming_direction_student: Mapped["IncomingDirectionStudent"] = relationship(
    #     back_populates="incoming_direction"
    # )
    excel_rel: Mapped["IncomingDirection"] = relationship(
        back_populates="incoming_direction_files"  # ← имя атрибута в IncomingDirection
    )



class Student(Base):
    __tablename__ = 'student'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    full_name: Mapped[str] = mapped_column(
        String(300), nullable=False, comment='ФИО'
    )
    specialization_id: Mapped[int] = mapped_column(
        ForeignKey('specialization.id'), nullable=True,
        comment='ID Специализации из пдф'
    )
    incoming_direction_id: Mapped[int] = mapped_column(
        ForeignKey('incoming_direction.id'), nullable=True,
        comment='ID напраления из пдф'
    )

    file_name: Mapped[str] = mapped_column(
        String(500), nullable=True, comment='Код файла пдф'
    )
    file_code: Mapped[str] = mapped_column(
        String(500), nullable=True, unique=True, comment='Код файла'
    )

    specialization: Mapped["Specialization"] = relationship(
        back_populates="students"
    )

    inc_student: Mapped["IncomingDirection"] = relationship(
        back_populates="incoming_direction_student"
    )
#     incoming_direction_students: Mapped[List["IncomingDirectionStudent"]] = relationship(
#         back_populates="student"  # ← имя атрибута в IncomingDirectionStudent
#     )

#     def __repr__(self) -> str:
#         return f"<Student(id={self.id}, full_name='{self.full_name}')>"
    
# class IncomingDirectionStudent(Base):
#     __tablename__ = 'incoming_direction_student'

#     id: Mapped[int] =  mapped_column(primary_key=True, autoincrement=True)
#     student_id: Mapped[int] = mapped_column(ForeignKey('student.id'), nullable=False)
#     incoming_direction_id: Mapped[int] = mapped_column(ForeignKey('incoming_direction.id'), nullable=False)

#     # student: Mapped[List["Student"]] = relationship(
#     #     back_populates="incoming_direction_student"
#     # )
#     # direction: Mapped["IncomingDirection"] = relationship(
#     #     back_populates="incoming_direction_student"
#     # )

#     student: Mapped["Student"] = relationship(
#         back_populates="incoming_direction_students"  # ← имя атрибута в Student
#     )
#     # MANY-to-ONE → один объект (FK здесь)
#     direction: Mapped["IncomingDirection"] = relationship(
#         back_populates="incoming_direction_students"  # ← имя атрибута в IncomingDirection
#     )



# class Student(Base):
#     __tablename__ = 'student'

#     id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
#     full_name: Mapped[str] = mapped_column(
#         String(300), nullable=False, comment='ФИО'
#     )
#     specialization_id: Mapped[int] = mapped_column(
#         ForeignKey('specialization.id'), nullable=False,
#         comment='ID Специализации'
#     )

#     file_name: Mapped[str] = mapped_column(
#         String(500), nullable=False, comment='Код файла'
#     )

#     specialization: Mapped["Specialization"] = relationship(
#         back_populates="students"
#     )

#     def __repr__(self) -> str:
#         return f"<Student(id={self.id}, full_name='{self.full_name}')>"

class FormatControl(Base):
    __tablename__ = 'format_control'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    format_name: Mapped[str] = mapped_column(
        String(300), nullable=False, comment='Формат контроля (зачет/экзамен) с листа переаттестация эксель'
    )

    control_tables_norma: Mapped[List["ControlTable"]] = relationship(
        back_populates="format_control_norma",
        foreign_keys="[ControlTable.format_control_norma_id]"
    )
    control_tables_fact: Mapped[List["ControlTable"]] = relationship(
        back_populates="format_control_fact",
        foreign_keys="[ControlTable.format_control_fact_id]"
    )

    def __repr__(self) -> str:
        return f"<FormatControl(id={self.id}, format_name='{self.format_name}')>"


class FormatRetests(Base):
    __tablename__ = 'format_retests'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    format_name: Mapped[str] = mapped_column(
        String(300), nullable=False, comment='Формат переатестации например Переаттестовано(частично) с листа переаттестация эксель'
    )

    control_tables: Mapped[List["ControlTable"]] = relationship(
        back_populates="format_retests"
    )
    
    def __repr__(self) -> str:
        return f"<FormatRetests(id={self.id}, format_name='{self.format_name}')>"


class StudyProgram(Base):
    __tablename__ = 'study_program'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(300), nullable=False, comment='Наименование предмета с листа переаттестация эксель'
    )

    control_tables: Mapped[List["ControlTable"]] = relationship(
        back_populates="study_program"
    )
    
    def __repr__(self) -> str:
        return f"<StudyProgram(id={self.id}, name='{self.name}')>"


class ControlTable(Base):
    __tablename__ = 'control_table'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    incoming_direction_id: Mapped[int] = mapped_column(
        ForeignKey('incoming_direction.id'), nullable=False,
        comment='ID напраления'
    )

    study_program_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey('study_program.id'), nullable=True,
        comment='ID Предмета'
    )
    format_control_norma_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey('format_control.id'), nullable=True,
        comment='ID Формат контроля НОРМА'
    )
    format_control_fact_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey('format_control.id'), nullable=True,
        comment='ID Формат контроля ФАКТ'
    )
    format_retests_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey('format_retests.id'), nullable=True,
        comment='ID Формат переатестации'
    )
    hours_fact: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True, comment='Часы ФАКТ (Изучено/зачтено)'
    )
    hours_normal: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True, comment='Часы По плану'
    )

    # specialization: Mapped[Optional["Specialization"]] = relationship(
    #     back_populates="control_tables"
    # )
    study_program: Mapped[Optional["StudyProgram"]] = relationship(
        back_populates="control_tables"
    )
    format_control_norma: Mapped[Optional["FormatControl"]] = relationship(
        back_populates="control_tables_norma",
        foreign_keys=[format_control_norma_id]
    )
    format_control_fact: Mapped[Optional["FormatControl"]] = relationship(
        back_populates="control_tables_fact",
        foreign_keys=[format_control_fact_id]
    )
    format_retests: Mapped[Optional["FormatRetests"]] = relationship(
        back_populates="control_tables"
    )
    inc_cntrl_table: Mapped["IncomingDirection"] = relationship(
        back_populates="incoming_direction_table"
    )
    
    def __repr__(self) -> str:
        program_name = self.study_program.name if self.study_program else "None"
        return f"<ControlTable(id={self.id}, program='{program_name}')>"