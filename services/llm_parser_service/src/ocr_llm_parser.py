from langchain_gigachat.chat_models import GigaChat
from pydantic import BaseModel, Field
import logging
from typing import List
from dataclasses import dataclass, field

# from parser_service.config import service_config
logging.basicConfig(
    level=logging.INFO,  # Уровень: DEBUG, INFO, WARNING, ERROR
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class StudentInfo(BaseModel):
    full_name: str = Field(default=None, description="ФИО студента. Имя фамилия и отчество при наличии вместе через пробел")
    direction: str = Field(default=None, description="Это квалификация студента")
    specialization: str = Field(default=None, description="Это специальность на которой учился студент")
    university: str = Field(default=None, description="Название университета")

class ParsedStudent(StudentInfo):
    code: str | None = None
    errors: List[str] = []
    
    @property
    def is_valid(self) -> bool:
        return all([
            self.full_name,
            self.direction,
            self.university,
            self.specialization
        ])

    @property
    def missing_fields(self) -> List[str]:
        missing = []
        if not self.full_name:
            missing.append("ФИО студента")
        if not self.direction:
            missing.append("Квалификация (направление)")
        if not self.university:
            missing.append("Учебное заведение")
        if not self.specialization:
            missing.append("Специальность")
        return missing
    

class LLMParser:
    def __init__(self, model: GigaChat):
        self._model = model

    def parse_image_text(self, text: str):
        input_prompt = f"""ТВОЯ РОЛЬ ИЗВЛЕКАТЬ ДАННЫЕ ИЗ ТЕКСТА. 
        На вход тебе поступает текст с картинки который в нечитаемом формате.
        Твоя задача распознать в этом тексте:
        1. Фамилию Имя Отчетсво
        2. Квалификацию студента, например Дизайн, Бухгалтерия
        3. Специальность с кодом, например 38.02.01 Экономика и бухгалтерский учет
        4. Название университета
        Выдать все это в JSON формате 
        JSON должен выглдеть так:
        {{
            "full_name": "Иванов Иван Иванович",
            "direction": "Бухгалтерия",
            "specialization": "38.02.01 Экономика и бухгалтерский учет",
            "university": "ФГБОУ им. Г.В. Плеханова",
        }}

        Вот текст который надо распознать:
        {text}
        """
        parsed_student = ParsedStudent()
        try:
            logger.info("Structing LLM")
            structed_llm = self._model.with_structured_output(StudentInfo, method="json_mode",include_raw=True)
        except Exception as e:
            logger.error(f"Error in structing LLM: {e}")
            parsed_student.errors.append(f"Error in structing LLM: {e}")

        try:
            logger.info("Invoke LLM")
            response = structed_llm.invoke(input_prompt)
            splited = LLMParser.split_code(response["parsed"].specialization)
            parsed = response["parsed"]
            if isinstance(splited, dict):
                parsed_student.code = splited["code"]
                parsed_student.specialization = splited["name"]
            else:
                parsed_student.code = None
                parsed_student.specialization = parsed.specialization
            parsed_student.full_name = parsed.full_name
            parsed_student.direction=parsed.direction
            parsed_student.university=parsed.university

            return parsed_student
        except Exception as e:
            logger.error(f"Error in answer LLM: {e}")
            parsed_student.errors.append(f"Error in answer LLM: {e}")

    def split_code(specialization: str) -> dict:
        import re

        pattern = r'^([\d\.]+)\s+(.+)$'

        match = re.match(pattern, specialization)
        if match:
            code = match.group(1) 
            name = match.group(2)  
            print(f"Код: {code}")
            print(f"Название: {name}")
            return {"code": code, "name": name}
        else:
            # Если не подошло, оставляем всю строку как есть
            return specialization