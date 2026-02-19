from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship, declarative_base
from typing import List

Base = declarative_base()

# Направление
class Direction(Base):
    __tablename__ = 'directions'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, comment='Наименование направления')
    
    specializations: Mapped[List["Specialization"]] = relationship(back_populates="direction")
    
    def __repr__(self) -> str:
        return f"<Direction(id={self.id}, name='{self.name}')>"

# Учебное заведение
class University(Base):
    __tablename__ = 'university'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, comment='Наименование учебного заведения')
    
    specializations: Mapped[List["Specialization"]] = relationship(back_populates="university")
    
    def __repr__(self) -> str:
        return f"<University(id={self.id}, name='{self.name}')>"

# Специализация
class Specialization(Base):
    __tablename__ = 'specialization'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, comment='Наименование специальности')
    direction_id: Mapped[int] = mapped_column(ForeignKey('directions.id'), nullable=False, comment='ID Направления')
    university_id: Mapped[int] = mapped_column(ForeignKey('university.id'), nullable=False, comment='ID Учебного заведения')
    
    direction: Mapped["Direction"] = relationship(back_populates="specializations")
    university: Mapped["University"] = relationship(back_populates="specializations")
    students: Mapped[List["Student"]] = relationship(back_populates="specialization")
    control_tables: Mapped[List["ControlTable"]] = relationship(back_populates="specialization")

    def __repr__(self) -> str:
        return f"<Specialization(id={self.id}, name='{self.name}')>"
    
# Студент
class Student(Base):
    __tablename__ = 'student'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    full_name: Mapped[str] = mapped_column(String(300), nullable=False, comment='ФИО')
    specialization_id: Mapped[int] = mapped_column(ForeignKey('specialization.id'), nullable=False, comment='ID Специализации')
    
    specialization: Mapped["Specialization"] = relationship(back_populates="students")
    
    def __repr__(self) -> str:
        return f"<Student(id={self.id}, full_name='{self.full_name}')>"
    
# Форма итогового контроля
class FormatControl(Base):
    __tablename__ = 'format_control'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    format_name: Mapped[str] = mapped_column(String(300), nullable=False, comment='Формат контроля')

    control_tables_norma: Mapped[List["ControlTable"]] = relationship(back_populates="format_control_norma")
    control_tables_fact: Mapped[List["ControlTable"]] = relationship(back_populates="format_control_fact")
    
    def __repr__(self) -> str:
        return f"<FormatControl(id={self.id}, format_name='{self.format_name}')>"
    
# Форма переатестации
class FormatRetests(Base):
    __tablename__ = 'format_retests'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    format_name: Mapped[str] = mapped_column(String(300), nullable=False, comment='Формат переатестации')

    control_tables: Mapped[List["ControlTable"]] = relationship(back_populates="format_retests")

# Учебный предмет
class StudyProgram(Base):
    __tablename__ = 'study_program'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(300), nullable=False, comment='Наименование предмета')

    control_tables: Mapped[List["ControlTable"]] = relationship(back_populates="study_program")

# Сводная таблица
class ControlTable(Base):
    __tablename__ = 'control_table'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    specialization_id: Mapped[int] = mapped_column(ForeignKey('specialization.id'), nullable=False, comment='ID Специализации')
    study_program_id: Mapped[int] = mapped_column(ForeignKey('study_program.id'), nullable=False, comment='ID Предмета')
    format_control_norma_id: Mapped[int] = mapped_column(ForeignKey('format_control.id'), nullable=False, comment='ID Формат контроля НОРМА')
    format_control_fact_id: Mapped[int] = mapped_column(ForeignKey('format_control.id'), nullable=False, comment='ID Формат контроля ФАКТ')
    format_retests_id: Mapped[int] = mapped_column(ForeignKey('format_retests.id'), nullable=False, comment='ID Формат переатестации')
    hours_fact: Mapped[str] = mapped_column(String(200), nullable=False, comment='Часы ФАКТ')
    hours_normal: Mapped[str] = mapped_column(String(200), nullable=False, comment='Часы НОРМА')

    specialization: Mapped["Specialization"] = relationship(back_populates="control_tables")
    study_program: Mapped["StudyProgram"] = relationship(back_populates="control_tables")
    format_control_norma: Mapped["FormatControl"] = relationship(back_populates="control_tables_norma", foreign_keys=[format_control_norma_id])
    format_control_fact: Mapped["FormatControl"] = relationship(back_populates="control_tables_fact", foreign_keys=[format_control_fact_id])
    format_retests: Mapped["FormatRetests"] = relationship(back_populates="control_tables")