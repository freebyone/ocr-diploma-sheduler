import re
import logging
from typing import List, Tuple, Optional, Dict, Any
from sqlalchemy.orm import Session

# Меняем на абсолютные импорты
from app.models import (
    University, Direction, Specialization, 
    FormatControl, FormatRetests, StudyProgram, ControlTable
)

logger = logging.getLogger(__name__)


class WordParser:
    """Парсер для Word-документов с приказами о переаттестации"""
    
    def __init__(self, session: Session):
        self.session = session
        self.stats = {
            'universities': 0,
            'directions': 0,
            'specializations': 0,
            'study_programs': 0,
            'control_tables': 0,
            'errors': []
        }
    
    def get_or_create(self, model, **kwargs):
        """Получить существующую запись или создать новую"""
        try:
            instance = self.session.query(model).filter_by(**kwargs).first()
            if instance:
                return instance
            
            instance = model(**kwargs)
            self.session.add(instance)
            self.session.flush()
            
            # Обновляем статистику
            if model == University:
                self.stats['universities'] += 1
            elif model == Direction:
                self.stats['directions'] += 1
            elif model == Specialization:
                self.stats['specializations'] += 1
            elif model == StudyProgram:
                self.stats['study_programs'] += 1
            
            return instance
        except Exception as e:
            logger.error(f"Error in get_or_create for {model.__name__}: {e}")
            self.stats['errors'].append(f"get_or_create {model.__name__}: {str(e)}")
            raise
    
    def clean_text(self, text: str) -> str:
        """Очистка текста от лишних символов"""
        if not text:
            return ""
        text = re.sub(r'\*\*', '', text)
        text = re.sub(r'\\\*\*', '', text)
        return text.strip()
    
    def parse_block_header(self, line: str) -> Tuple[Optional[str], Optional[str]]:
        """Парсинг заголовка блока: извлечение университета и специальности"""
        patterns = [
            r'прослушанных в (.*?) по специальности «(.*?)»:',
            r'прослушанных в (.*?) по специальности "(.*?)":',
            r'прослушанных в (.*?) по специальности (.*?):'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                university = self.clean_text(match.group(1))
                speciality = self.clean_text(match.group(2))
                return university, speciality
        
        return None, None
    
    def parse_table_row(self, row: str) -> List[str]:
        """Парсинг строки таблицы"""
        parts = [self.clean_text(p) for p in row.split('|')]
        if parts and parts[0] == '':
            parts = parts[1:]
        return parts
    
    def process_table(self, table_lines: List[str]) -> List[List[str]]:
        """Обработка таблицы с учетом многострочных названий дисциплин"""
        data_lines = [ln for ln in table_lines if ln.startswith('|')]
        records = []
        current = None
        
        for ln in data_lines:
            parts = self.parse_table_row(ln)
            
            if parts and parts[0] and parts[0] not in ('*', '**', '\\**'):
                if current:
                    records.append(current)
                current = parts
            else:
                if current and parts:
                    current[0] += ' ' + parts[0]
        
        if current:
            records.append(current)
        
        return [r[:7] for r in records if len(r) >= 7]
    
    def save_control_record(self, specialization: Specialization, record: List[str]):
        """Сохранение одной записи контрольной таблицы"""
        try:
            disc_name, hours_norm, form_norm, hours_fact, form_fact, _, form_retest = record
            
            if not disc_name or disc_name in ('*', '**', '\\**'):
                return
            
            program = self.get_or_create(StudyProgram, name=disc_name)
            fc_norm = self.get_or_create(FormatControl, format_name=form_norm or 'не указано')
            fc_fact = self.get_or_create(FormatControl, format_name=form_fact or 'не указано')
            fret = self.get_or_create(FormatRetests, format_name=form_retest or 'не указано')
            
            control = ControlTable(
                specialization_id=specialization.id,
                study_program_id=program.id,
                format_control_norma_id=fc_norm.id,
                format_control_fact_id=fc_fact.id,
                format_retests_id=fret.id,
                hours_fact=hours_fact or '0',
                hours_normal=hours_norm or '0'
            )
            
            self.session.add(control)
            self.stats['control_tables'] += 1
            
        except Exception as e:
            logger.error(f"Error saving control record: {e}, record: {record}")
            self.stats['errors'].append(f"save_control_record: {str(e)}")
    
    def parse_document(self, text_lines: List[str]) -> Dict[str, Any]:
        """Основной метод парсинга документа"""
        i = 0
        total_lines = len(text_lines)
        
        while i < total_lines:
            line = text_lines[i].strip()
            
            if line.startswith('прослушанных в ') and line.endswith(':'):
                university_name, spec_name = self.parse_block_header(line)
                
                if not university_name or not spec_name:
                    logger.warning(f"Could not parse header: {line}")
                    i += 1
                    continue
                
                logger.info(f"Processing block: {university_name} - {spec_name}")
                
                i += 1
                while i < total_lines and not text_lines[i].strip().startswith('+'):
                    i += 1
                
                if i >= total_lines:
                    break
                
                table_lines = []
                while i < total_lines and not text_lines[i].strip().startswith('прослушанных в '):
                    table_lines.append(text_lines[i].strip())
                    i += 1
                
                university = self.get_or_create(University, name=university_name)
                direction = self.get_or_create(Direction, name=spec_name)
                
                specialization = self.get_or_create(
                    Specialization,
                    name=spec_name,
                    code='',
                    direction_id=direction.id,
                    university_id=university.id
                )
                
                records = self.process_table(table_lines)
                
                for record in records:
                    self.save_control_record(specialization, record)
                
                try:
                    self.session.commit()
                    logger.info(f"Block processed: {len(records)} records saved")
                except Exception as e:
                    self.session.rollback()
                    logger.error(f"Error committing block: {e}")
                    self.stats['errors'].append(f"commit block: {str(e)}")
            else:
                i += 1
        
        return self.stats