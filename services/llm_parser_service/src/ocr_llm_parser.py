from langchain_gigachat.chat_models import GigaChat
from pydantic import BaseModel, Field
import logging
import re
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class StudentInfo(BaseModel):
    full_name: Optional[str] = Field(
        default=None,
        description="ФИО студента. Фамилия имя и отчество при наличии вместе через пробел"
    )
    direction: Optional[str] = Field(
        default=None,
        description="Это квалификация студента"
    )
    specialization: Optional[str] = Field(
        default=None,
        description="Это специальность на которой учился студент"
    )
    university: Optional[str] = Field(
        default=None,
        description="Название университета"
    )


class ParsedStudent(BaseModel):
    full_name: Optional[str] = None
    direction: Optional[str] = None
    specialization: Optional[str] = None
    university: Optional[str] = None
    code: Optional[str] = None
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

    def parse_image_text(self, text: str) -> ParsedStudent:
        """
        Парсит OCR текст через LLM.
        ВСЕГДА возвращает ParsedStudent (никогда не None).
        """
        input_prompt = f"""ТВОЯ РОЛЬ ИЗВЛЕКАТЬ ДАННЫЕ ИЗ ТЕКСТА. 
        На вход тебе поступает текст с картинки который в нечитаемом формате.
        Твоя задача распознать в этом тексте:
        1. Фамилию Имя Отчество
        2. Квалификацию студента, например Дизайнер, Бухгалтер, Юрист
        3. Специальность с кодом, например 38.02.01 Экономика и бухгалтерский учет
        4. Название университета
        Выдать все это в JSON формате 
        JSON должен выглядеть так:
        {{
            "full_name": "Иванов Иван Иванович",
            "direction": "Бухгалтер",
            "specialization": "38.02.01 Экономика и бухгалтерский учет",
            "university": "ФГБОУ им. Г.В. Плеханова"
        }}

        Вот текст который надо распознать:
        {text}
        """

        parsed_student = ParsedStudent()

        # 1. Создаём structured LLM
        try:
            logger.info("Structuring LLM")
            structured_llm = self._model.with_structured_output(
                StudentInfo,
                method="json_mode",
                include_raw=True
            )
        except Exception as e:
            error_msg = f"Error structuring LLM: {e}"
            logger.error(error_msg)
            parsed_student.errors.append(error_msg)
            return parsed_student  # ← ВОЗВРАЩАЕМ, а не проваливаемся дальше

        # 2. Вызываем LLM
        try:
            logger.info("Invoking LLM")
            response = structured_llm.invoke(input_prompt)

            logger.info(f"LLM raw response type: {type(response)}")
            logger.info(f"LLM raw response: {response}")

            # Проверяем что response — это dict с ключом "parsed"
            if isinstance(response, dict):
                parsed = response.get("parsed")
                raw = response.get("raw")

                if raw:
                    logger.info(f"LLM raw output: {raw}")

                if parsed is None:
                    error_msg = (
                        f"LLM returned dict but 'parsed' is None. "
                        f"Keys: {list(response.keys())}"
                    )
                    logger.error(error_msg)

                    # Попробуем извлечь из parsing_error
                    parsing_error = response.get("parsing_error")
                    if parsing_error:
                        logger.error(f"Parsing error: {parsing_error}")
                        parsed_student.errors.append(
                            f"LLM parsing error: {parsing_error}"
                        )
                    else:
                        parsed_student.errors.append(error_msg)

                    return parsed_student

            elif isinstance(response, StudentInfo):
                # Если вернулся напрямую объект (без include_raw)
                parsed = response
            else:
                error_msg = (
                    f"Unexpected response type: {type(response)}. "
                    f"Value: {response}"
                )
                logger.error(error_msg)
                parsed_student.errors.append(error_msg)
                return parsed_student

            # 3. Заполняем ParsedStudent
            logger.info(f"Parsed StudentInfo: {parsed}")

            parsed_student.full_name = parsed.full_name
            parsed_student.direction = parsed.direction
            parsed_student.university = parsed.university

            # 4. Разделяем код и название специальности
            if parsed.specialization:
                splited = LLMParser.split_code(parsed.specialization)
                if isinstance(splited, dict):
                    parsed_student.code = splited["code"]
                    parsed_student.specialization = splited["name"]
                else:
                    parsed_student.code = None
                    parsed_student.specialization = parsed.specialization
            else:
                parsed_student.specialization = None
                parsed_student.code = None

            logger.info(f"Final parsed result:")
            logger.info(f"  full_name:      {parsed_student.full_name}")
            logger.info(f"  direction:      {parsed_student.direction}")
            logger.info(f"  university:     {parsed_student.university}")
            logger.info(f"  specialization: {parsed_student.specialization}")
            logger.info(f"  code:           {parsed_student.code}")
            logger.info(f"  is_valid:       {parsed_student.is_valid}")

            return parsed_student

        except Exception as e:
            error_msg = f"Error invoking LLM: {e}"
            logger.error(error_msg)
            import traceback
            logger.error(traceback.format_exc())
            parsed_student.errors.append(error_msg)
            return parsed_student  # ← ВСЕГДА возвращаем объект

    @staticmethod
    def split_code(specialization: str) -> dict | str:
        """
        '38.02.01 Экономика и бухгалтерский учет'
        → {'code': '38.02.01', 'name': 'Экономика и бухгалтерский учет'}
        
        'Просто название'
        → 'Просто название'
        """
        pattern = r'^([\d\.]+)\s+(.+)$'
        match = re.match(pattern, specialization.strip())
        if match:
            code = match.group(1)
            name = match.group(2)
            logger.info(f"Split code: {code} | name: {name}")
            return {"code": code, "name": name}
        else:
            return specialization