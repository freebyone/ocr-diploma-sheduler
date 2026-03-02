import re
from typing import Optional, Dict, List, Tuple, Any
import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
import logging

# Исправленные импорты для работы в структуре src
from src.models import (
    Base, Direction, Specialization, 
    StudyProgram, FormatControl, FormatRetests, ControlTable
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XLSXParser:
    def __init__(self, file_path: str, db_url: str, sheets: List[str] = ["Титул", "Переаттестация"]):
        self.file_path = file_path
        self.wb = pd.read_excel(file_path, sheet_name=[])
        
        # Настройка подключения к БД
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)