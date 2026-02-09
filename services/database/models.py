from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from datetime import datetime
import logging

Base = declarative_base()

# Таблица Направление
class Direction(Base):
    __tablename__ = 'directions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    speciality_code = Column(String(50), nullable=False, comment='Код специальности')
    name = Column(String(200), nullable=False, comment='Наименование направления')
    department = Column(String(100), nullable=False, comment='Кафедра')
    faculty = Column(String(100), nullable=False, comment='Факультет')
    
    students = relationship("Student", back_populates="direction")
    hour_norms = relationship("HourNorm", back_populates="direction")
    
    def __repr__(self):
        return f"<Direction(id={self.id}, code='{self.speciality_code}', name='{self.name}')>"

# Таблица УчебноеЗаведение
class EducationalInstitution(Base):
    __tablename__ = 'educational_institutions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False, comment='Наименование')
    year = Column(Integer, nullable=False, comment='Год основания')
    
    students = relationship("Student", back_populates="institution")
    
    def __repr__(self):
        return f"<EducationalInstitution(id={self.id}, name='{self.name}', year={self.year})>"

# Таблица Студент
class Student(Base):
    __tablename__ = 'students'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(200), nullable=False, comment='ФИО')
    direction_id = Column(Integer, ForeignKey('directions.id'), nullable=False, comment='ID направления')
    speciality_code = Column(String(50), nullable=False, comment='Код специальности')
    institution_id = Column(Integer, ForeignKey('educational_institutions.id'), nullable=False, comment='ID учебного заведения')
    
    direction = relationship("Direction", back_populates="students")
    institution = relationship("EducationalInstitution", back_populates="students")
    student_subjects = relationship("StudentSubject", back_populates="student")
    
    def __repr__(self):
        return f"<Student(id={self.id}, name='{self.full_name}', direction_id={self.direction_id})>"

# Таблица Предмет
class Subject(Base):
    __tablename__ = 'subjects'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False, comment='Имя предмета')
    
    hour_norms = relationship("HourNorm", back_populates="subject")
    student_subjects = relationship("StudentSubject", back_populates="subject")
    
    def __repr__(self):
        return f"<Subject(id={self.id}, name='{self.name}')>"

# Таблица НормаЧасов
class HourNorm(Base):
    __tablename__ = 'hour_norms'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    subject_id = Column(Integer, ForeignKey('subjects.id'), nullable=False, comment='ID предмета')
    direction_id = Column(Integer, ForeignKey('directions.id'), nullable=False, comment='ID направления')
    hours = Column(Integer, nullable=False, comment='Часы')
    
    subject = relationship("Subject", back_populates="hour_norms")
    direction = relationship("Direction", back_populates="hour_norms")
    
    def __repr__(self):
        return f"<HourNorm(id={self.id}, subject_id={self.subject_id}, direction_id={self.direction_id}, hours={self.hours})>"

# Таблица ПредметУченик
class StudentSubject(Base):
    __tablename__ = 'student_subjects'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey('students.id'), nullable=False, comment='ID студента')
    subject_id = Column(Integer, ForeignKey('subjects.id'), nullable=False, comment='ID предмета')
    hours = Column(Integer, nullable=False, comment='Часы')
    
    student = relationship("Student", back_populates="student_subjects")
    subject = relationship("Subject", back_populates="student_subjects")
    
    def __repr__(self):
        return f"<StudentSubject(id={self.id}, student_id={self.student_id}, subject_id={self.subject_id}, hours={self.hours})>"