from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker, joinedload
from models import Base, Direction, Student, EducationalInstitution, Subject, HourNorm, StudentSubject
from typing import List, Optional, Dict, Any
import logging

class DatabaseService:
    def __init__(self, database_url: str = "postgresql://ocr_user:ocr_password@localhost:5432/norma_db"):
        self.engine = create_engine(database_url, echo=False)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(__name__)
        
    def create_tables(self):
        try:
            Base.metadata.create_all(self.engine)
            self.logger.info("Таблицы успешно созданы")
        except Exception as e:
            self.logger.error(f"Ошибка при создании таблиц: {e}")
            raise
    
    def drop_tables(self):
        try:
            Base.metadata.drop_all(self.engine)
            self.logger.info("Таблицы успешно удалены")
        except Exception as e:
            self.logger.error(f"Ошибка при удалении таблиц: {e}")
            raise
    
    # CRUD операции для Direction
    def create_direction(self, speciality_code: str, name: str, department: str, faculty: str) -> Direction:
        with self.Session() as session:
            direction = Direction(
                speciality_code=speciality_code,
                name=name,
                department=department,
                faculty=faculty
            )
            session.add(direction)
            session.commit()
            session.refresh(direction)
            return direction
    
    def get_direction(self, direction_id: int) -> Optional[Direction]:
        with self.Session() as session:
            return session.query(Direction).get(direction_id)
    
    def get_all_directions(self) -> List[Direction]:
        with self.Session() as session:
            return session.query(Direction).all()
    
    def update_direction(self, direction_id: int, **kwargs) -> Optional[Direction]:
        with self.Session() as session:
            direction = session.query(Direction).get(direction_id)
            if direction:
                for key, value in kwargs.items():
                    if hasattr(direction, key):
                        setattr(direction, key, value)
                session.commit()
                session.refresh(direction)
            return direction
    
    def delete_direction(self, direction_id: int) -> bool:
        with self.Session() as session:
            direction = session.query(Direction).get(direction_id)
            if direction:
                session.delete(direction)
                session.commit()
                return True
            return False
    
    # CRUD операции для Student
    def create_student(self, full_name: str, direction_id: int, 
                      speciality_code: str, institution_id: int) -> Student:
        with self.Session() as session:
            student = Student(
                full_name=full_name,
                direction_id=direction_id,
                speciality_code=speciality_code,
                institution_id=institution_id
            )
            session.add(student)
            session.commit()
            session.refresh(student)
            return student
    
    def get_student_with_details(self, student_id: int) -> Optional[Dict]:
        with self.Session() as session:
            student = session.query(Student)\
                .options(
                    joinedload(Student.direction),
                    joinedload(Student.institution),
                    joinedload(Student.student_subjects).joinedload(StudentSubject.subject)
                )\
                .get(student_id)
            
            if student:
                return {
                    'id': student.id,
                    'full_name': student.full_name,
                    'direction': {
                        'id': student.direction.id,
                        'name': student.direction.name,
                        'speciality_code': student.direction.speciality_code
                    },
                    'institution': {
                        'id': student.institution.id,
                        'name': student.institution.name
                    },
                    'subjects': [
                        {
                            'subject_id': ss.subject_id,
                            'subject_name': ss.subject.name,
                            'hours': ss.hours
                        }
                        for ss in student.student_subjects
                    ]
                }
            return None
    
    def get_students_by_direction(self, direction_id: int) -> List[Student]:
        with self.Session() as session:
            return session.query(Student)\
                .filter(Student.direction_id == direction_id)\
                .all()
    
    def create_institution(self, name: str, year: int) -> EducationalInstitution:
        with self.Session() as session:
            institution = EducationalInstitution(
                name=name,
                year=year
            )
            session.add(institution)
            session.commit()
            session.refresh(institution)
            return institution
    
    def create_subject(self, name: str) -> Subject:
        with self.Session() as session:
            subject = Subject(name=name)
            session.add(subject)
            session.commit()
            session.refresh(subject)
            return subject
    
    def create_hour_norm(self, subject_id: int, direction_id: int, hours: int) -> HourNorm:
        with self.Session() as session:
            hour_norm = HourNorm(
                subject_id=subject_id,
                direction_id=direction_id,
                hours=hours
            )
            session.add(hour_norm)
            session.commit()
            session.refresh(hour_norm)
            return hour_norm
    
    def get_norms_by_direction(self, direction_id: int) -> List[HourNorm]:
        with self.Session() as session:
            return session.query(HourNorm)\
                .options(joinedload(HourNorm.subject))\
                .filter(HourNorm.direction_id == direction_id)\
                .all()
    
    def create_student_subject(self, student_id: int, subject_id: int, hours: int) -> StudentSubject:
        with self.Session() as session:
            student_subject = StudentSubject(
                student_id=student_id,
                subject_id=subject_id,
                hours=hours
            )
            session.add(student_subject)
            session.commit()
            session.refresh(student_subject)
            return student_subject
    
    def get_student_hours_summary(self, student_id: int) -> Dict:
        with self.Session() as session:
            student_subjects = session.query(StudentSubject)\
                .options(joinedload(StudentSubject.subject))\
                .filter(StudentSubject.student_id == student_id)\
                .all()
            
            total_hours = sum(ss.hours for ss in student_subjects)
            
            return {
                'student_id': student_id,
                'total_subjects': len(student_subjects),
                'total_hours': total_hours,
                'subjects': [
                    {
                        'subject_id': ss.subject_id,
                        'subject_name': ss.subject.name,
                        'hours': ss.hours
                    }
                    for ss in student_subjects
                ]
            }
    
    def compare_hours_with_norms(self, student_id: int) -> Dict:
        with self.Session() as session:
            student = session.query(Student)\
                .options(joinedload(Student.direction))\
                .get(student_id)
            
            if not student:
                return {'error': 'Student not found'}
            
            norms = session.query(HourNorm)\
                .options(joinedload(HourNorm.subject))\
                .filter(HourNorm.direction_id == student.direction_id)\
                .all()
            
            actual_hours = session.query(StudentSubject)\
                .options(joinedload(StudentSubject.subject))\
                .filter(StudentSubject.student_id == student_id)\
                .all()
            
            actual_dict = {ah.subject_id: ah.hours for ah in actual_hours}
            
            comparison = []
            total_norm_hours = 0
            total_actual_hours = 0
            
            for norm in norms:
                actual = actual_dict.get(norm.subject_id, 0)
                difference = actual - norm.hours
                status = 'Норма' if difference >= 0 else 'Недостаточно'
                
                comparison.append({
                    'subject_id': norm.subject_id,
                    'subject_name': norm.subject.name,
                    'norm_hours': norm.hours,
                    'actual_hours': actual,
                    'difference': difference,
                    'status': status
                })
                
                total_norm_hours += norm.hours
                total_actual_hours += actual
            
            return {
                'student_id': student_id,
                'student_name': student.full_name,
                'direction': student.direction.name,
                'comparison': comparison,
                'summary': {
                    'total_norm_hours': total_norm_hours,
                    'total_actual_hours': total_actual_hours,
                    'total_difference': total_actual_hours - total_norm_hours
                }
            }