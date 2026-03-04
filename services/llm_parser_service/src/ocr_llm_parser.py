from langchain_gigachat.chat_models import GigaChat
from pydantic import BaseModel, Field
import logging
import re
import json
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================
# Models
# ============================================================

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


# ============================================================
# LLM Parser
# ============================================================

class LLMParser:

    def __init__(self, model: GigaChat):
        self._model = model

    # -------------------------------
    # Main method
    # -------------------------------

    def parse_image_text(self, text: str) -> ParsedStudent:
        """
        Парсит OCR текст через LLM.
        ВСЕГДА возвращает ParsedStudent.
        """

        input_prompt = f"""
ТВОЯ РОЛЬ — ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ ТЕКСТА.

Извлеки строго в JSON формате:

1. Фамилию Имя Отчество
2. Квалификацию студента (например: Юрист, Бухгалтер)
3. Специальность с кодом (например: 38.02.01 Экономика и бухгалтерский учет)
4. Название университета

ВЕРНИ ТОЛЬКО ЧИСТЫЙ JSON.
БЕЗ markdown.
БЕЗ пояснений.
БЕЗ текста вне JSON.

Формат:
{{
    "full_name": "Иванов Иван Иванович",
    "direction": "Бухгалтер",
    "specialization": "38.02.01 Экономика и бухгалтерский учет",
    "university": "ФГБОУ им. Г.В. Плеханова"
}}

Текст:
{text}
"""

        parsed_student = ParsedStudent()

        # 1️⃣ Вызов LLM
        try:
            logger.info("Invoking LLM")
            response = self._model.invoke(input_prompt)
            raw_content = response.content if hasattr(response, "content") else str(response)

            logger.info(f"LLM raw output:\n{raw_content}")

        except Exception as e:
            error_msg = f"Error invoking LLM: {e}"
            logger.error(error_msg)
            parsed_student.errors.append(error_msg)
            return parsed_student

        # 2️⃣ Извлекаем JSON
        data = self.extract_json(raw_content)

        if not data:
            parsed_student.errors.append("Не удалось извлечь JSON из ответа LLM")
            return parsed_student

        # 3️⃣ Заполняем объект
        parsed_student.full_name = self.clean_text(data.get("full_name"))
        parsed_student.direction = self.clean_text(data.get("direction"))
        parsed_student.university = self.clean_text(data.get("university"))

        specialization_raw = self.clean_text(data.get("specialization"))

        if specialization_raw:
            splitted = self.split_code(specialization_raw)
            if isinstance(splitted, dict):
                parsed_student.code = splitted["code"]
                parsed_student.specialization = splitted["name"]
            else:
                parsed_student.specialization = splitted

        logger.info("Final parsed result:")
        logger.info(f"  full_name:      {parsed_student.full_name}")
        logger.info(f"  direction:      {parsed_student.direction}")
        logger.info(f"  university:     {parsed_student.university}")
        logger.info(f"  specialization: {parsed_student.specialization}")
        logger.info(f"  code:           {parsed_student.code}")
        logger.info(f"  is_valid:       {parsed_student.is_valid}")

        return parsed_student

    # ============================================================
    # Helpers
    # ============================================================
    @staticmethod
    def extract_json(text: str) -> Optional[dict]:
        """
        Устойчивое извлечение JSON из ответа модели.
        Чинит:
        - типографские кавычки
        - control characters
        - markdown
        """

        try:
            # 1️⃣ Вырезаем JSON блок
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if not match:
                return None

            json_str = match.group(0)

            # 2️⃣ Нормализация кавычек
            replacements = {
                "“": '"',
                "”": '"',
                "„": '"',
                "«": '"',
                "»": '"',
                "’": "'",
            }

            for bad, good in replacements.items():
                json_str = json_str.replace(bad, good)

            # 3️⃣ Удаляем control characters
            json_str = re.sub(r'[\x00-\x1F\x7F]', '', json_str)

            # 4️⃣ Убираем лишние запятые перед }
            json_str = re.sub(r',\s*}', '}', json_str)

            logger.info(f"Normalized JSON:\n{json_str}")

            return json.loads(json_str)

        except Exception as e:
            logger.error(f"JSON extraction error after normalization: {e}")
            return None

    @staticmethod
    def clean_text(text: Optional[str]) -> Optional[str]:
        """
        Очищает мусор из OCR:
        - лишние кавычки
        - переносы строк
        - двойные пробелы
        """

        if not text:
            return None

        text = text.strip()
        text = re.sub(r'[»«"\n]', '', text)
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    @staticmethod
    def split_code(specialization: str) -> dict | str:
        """
        '38.02.01 Экономика и бухгалтерский учет'
        → {'code': '38.02.01', 'name': 'Экономика и бухгалтерский учет'}
        """

        pattern = r'^([\d\.]+)\s+(.+)$'
        match = re.match(pattern, specialization.strip())

        if match:
            code = match.group(1)
            name = match.group(2)
            logger.info(f"Split code: {code} | name: {name}")
            return {"code": code, "name": name}

        return specialization