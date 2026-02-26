import re
import logging
from typing import Optional, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ParsedDiploma:
    full_name: Optional[str] = None
    direction: Optional[str] = None
    university: Optional[str] = None
    specialization: Optional[str] = None
    errors: List[str] = field(default_factory=list)

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


def _normalize_whitespace(text: str) -> str:
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


# ==================== ФИО ====================

def parse_full_name(ocr_text: str) -> Optional[str]:
    surname = None
    first_name = None
    patronymic = None

    # --- Фамилия ---
    # Вариант 1: в одной строке
    m = re.search(r'Фамилия\s*[:\-]?\s+([А-ЯЁа-яё][А-ЯЁа-яё\-]+)', ocr_text)
    if m:
        surname = m.group(1).strip()
    else:
        # Вариант 2: на следующей строке
        m = re.search(r'Фамилия\s*[:\-]?\s*\n+\s*([А-ЯЁа-яё][А-ЯЁа-яё\-]+)', ocr_text)
        if m:
            surname = m.group(1).strip()

    # --- Имя ---
    # Нужно аккуратно — "Имя" может встретиться в других контекстах
    # Ищем именно поле "Имя" рядом с "Фамилия"/"Отчество"
    m = re.search(r'(?<!\w)Имя\s*[:\-]?\s+([А-ЯЁа-яё][А-ЯЁа-яё\-]+)', ocr_text)
    if m:
        first_name = m.group(1).strip()
    else:
        m = re.search(r'(?<!\w)Имя\s*[:\-]?\s*\n+\s*([А-ЯЁа-яё][А-ЯЁа-яё\-]+)', ocr_text)
        if m:
            first_name = m.group(1).strip()

    # --- Отчество ---
    # Может быть "Отчество (при наличии)" или просто "Отчество"
    m = re.search(
        r'Отчество\s*(?:\(при\s+наличии\))?\s*[:\-]?\s+([А-ЯЁа-яё][А-ЯЁа-яё\-]+)',
        ocr_text
    )
    if m:
        patronymic = m.group(1).strip()
    else:
        m = re.search(
            r'Отчество\s*(?:\(при\s+наличии\))?\s*[:\-]?\s*\n+\s*([А-ЯЁа-яё][А-ЯЁа-яё\-]+)',
            ocr_text
        )
        if m:
            patronymic = m.group(1).strip()

    # Собираем
    if surname and first_name:
        parts = [surname, first_name]
        if patronymic:
            parts.append(patronymic)
        full_name = " ".join(parts)
        logger.info(f"Parsed full name: {full_name}")
        return full_name

    # Fallback: пробуем найти ФИО целиком
    # "Фамилия Имя Отчество" — три слова подряд с заглавных букв
    # рядом с ключевыми словами диплома
    logger.warning("Could not parse full name by fields, trying fallback")
    return None


# ==================== Квалификация (направление) ====================

def parse_direction(ocr_text: str) -> Optional[str]:
    # Паттерн 1: значение на следующей строке
    m = re.search(
        r'Квалификация\s*[:\-]?\s*\n+\s*([А-ЯЁа-яё][А-ЯЁа-яё\s\-]+)',
        ocr_text
    )
    if m:
        direction = m.group(1).strip()
        direction = direction.split('\n')[0].strip()
        direction = re.sub(r'\s+по\s*$', '', direction).strip()
        if direction:
            logger.info(f"Parsed direction: {direction}")
            return direction

    # Паттерн 2: в одной строке
    m = re.search(
        r'Квалификация\s*[:\-]?\s+([А-ЯЁа-яё][А-ЯЁа-яё\s\-]+)',
        ocr_text
    )
    if m:
        direction = m.group(1).strip()
        direction = direction.split('\n')[0].strip()
        direction = re.sub(r'\s+по\s*$', '', direction).strip()
        if direction:
            logger.info(f"Parsed direction: {direction}")
            return direction

    logger.warning("Could not parse direction (qualification)")
    return None


# ==================== Учебное заведение ====================

def parse_university(ocr_text: str) -> Optional[str]:

    edu_keywords = [
        'университет', 'колледж', 'институт', 'техникум', 'академия',
        'образовательное учреждение', 'образовательная организация',
        'профессионального образования', 'высшего образования',
        'среднего профессионального', 'образования',
        'организация профессионального', 'некоммерческая организация',
        'бюджетное образовательное', 'автономная некоммерческая',
        'федеральное государственное', 'училище'
    ]

    # --- Стратегия 1: блок после "РОССИЙСКАЯ ФЕДЕРАЦИЯ" до полей ФИО ---
    block_match = re.search(
        r'(?:РОССИЙСКАЯ\s*\n?\s*ФЕДЕРАЦИЯ|РОССИЙСКАЯ ФЕДЕРАЦИЯ)\s*\n'
        r'(.*?)'
        r'(?=\n\s*(?:Имя|Отчество)\b)',
        ocr_text,
        re.DOTALL | re.IGNORECASE
    )

    if block_match:
        block = block_match.group(1).strip()
        block_lower = block.lower()
        has_keyword = any(kw in block_lower for kw in edu_keywords)
        if has_keyword and len(block) > 10:
            university = _clean_university_name(block)
            if university:
                logger.info(f"Parsed university (strategy 1 — block): {university}")
                return university

    # --- Стратегия 2: сбор строк с ключевыми словами ---
    lines = ocr_text.split('\n')
    edu_block_lines = []
    in_edu_block = False

    skip_patterns = [
        'фамилия', 'имя', 'отчество', 'дата рождения',
        'сведения о', 'квалификация', 'специальност',
        'регистрационный', 'приложение', 'дата выдачи',
        'предыдущий документ', 'аттестат', 'срок освоения',
        'российская федерация'
    ]

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            if in_edu_block and edu_block_lines:
                break
            continue

        line_lower = line_stripped.lower()

        if any(p in line_lower for p in skip_patterns):
            if in_edu_block:
                break
            continue

        if any(kw in line_lower for kw in edu_keywords):
            in_edu_block = True
            edu_block_lines.append(line_stripped)
        elif in_edu_block:
            edu_block_lines.append(line_stripped)

    if edu_block_lines:
        raw_name = ' '.join(edu_block_lines)
        university = _clean_university_name(raw_name)
        if university:
            logger.info(f"Parsed university (strategy 2 — keywords): {university}")
            return university

    # --- Стратегия 3: регулярки ---
    patterns = [
        r'((?:Автономная|Федеральное|Государственное|Частное|Негосударственное)'
        r'\s+(?:некоммерческая|государственное|бюджетное)'
        r'\s+(?:организация|учреждение|образовательное)'
        r'[\s\S]*?(?:г\.?\s*о?\.?\s*[\w\-]+|$))',

        r'([\w\s\-]*?'
        r'(?:университет|колледж|институт|техникум|академия|училище)'
        r'[\s\S]*?(?:г\.?\s*о?\.?\s*[\w\-]+|$))',
    ]

    for pattern in patterns:
        m = re.search(pattern, ocr_text, re.IGNORECASE)
        if m:
            candidate = m.group(1).strip()
            university = _clean_university_name(candidate)
            if university and len(university) > 10:
                logger.info(f"Parsed university (strategy 3 — regex): {university}")
                return university

    logger.warning("Could not parse university")
    return None


def _clean_university_name(raw: str) -> Optional[str]:
    """Чистит название учебного заведения от мусора"""
    if not raw:
        return None

    name = raw.replace('\n', ' ')
    name = re.sub(r'\s+', ' ', name)
    name = name.strip(' \t\n\r.,;:')

    # Убираем "РОССИЙСКАЯ ФЕДЕРАЦИЯ"
    name = re.sub(
        r'^РОССИЙСКАЯ\s+ФЕДЕРАЦИЯ\s*', '', name, flags=re.IGNORECASE
    ).strip()

    # Убираем поля ФИО если прицепились
    name = re.sub(r'\s*Фамилия\s.*$', '', name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s*Имя\s.*$', '', name, flags=re.IGNORECASE).strip()

    if len(name) < 5:
        return None

    return name


# ==================== Специальность ====================

def parse_specialization(ocr_text: str) -> Optional[str]:
    # Паттерн 1: "специальности" + код + название
    m = re.search(
        r'специальност[ьи]\s*[:\-]?\s*\n?\s*'
        r'(\d{2}\.\d{2}\.\d{2}\s+[А-ЯЁа-яё][\w\s\-\(\)]+)',
        ocr_text,
        re.IGNORECASE
    )
    if m:
        spec = m.group(1).strip().split('\n')[0].strip()
        logger.info(f"Parsed specialization: {spec}")
        return spec

    # Паттерн 2: просто код XX.XX.XX + название
    m = re.search(
        r'(\d{2}\.\d{2}\.\d{2}\s+[А-ЯЁа-яё][\w\s\-\(\)]+)',
        ocr_text
    )
    if m:
        spec = m.group(1).strip().split('\n')[0].strip()
        logger.info(f"Parsed specialization (by code): {spec}")
        return spec

    logger.warning("Could not parse specialization")
    return None


# ==================== Главная функция ====================

def parse_first_page(ocr_text: str) -> ParsedDiploma:
    result = ParsedDiploma()

    if not ocr_text or not ocr_text.strip():
        result.errors.append("OCR текст пуст")
        return result

    text = _normalize_whitespace(ocr_text)

    logger.info("=" * 50)
    logger.info("Parsing first page of diploma")
    logger.info(f"Text length: {len(text)} chars")

    # 1. ФИО
    try:
        result.full_name = parse_full_name(text)
        if not result.full_name:
            result.errors.append("Не удалось распознать ФИО студента")
    except Exception as e:
        logger.error(f"Error parsing full name: {e}")
        result.errors.append(f"Ошибка парсинга ФИО: {str(e)}")

    # 2. Квалификация (направление)
    try:
        result.direction = parse_direction(text)
        if not result.direction:
            result.errors.append("Не удалось распознать квалификацию (направление)")
    except Exception as e:
        logger.error(f"Error parsing direction: {e}")
        result.errors.append(f"Ошибка парсинга квалификации: {str(e)}")

    # 3. Учебное заведение
    try:
        result.university = parse_university(text)
        if not result.university:
            result.errors.append("Не удалось распознать учебное заведение")
    except Exception as e:
        logger.error(f"Error parsing university: {e}")
        result.errors.append(f"Ошибка парсинга учебного заведения: {str(e)}")

    # 4. Специальность
    try:
        result.specialization = parse_specialization(text)
        if not result.specialization:
            result.errors.append("Не удалось распознать специальность")
    except Exception as e:
        logger.error(f"Error parsing specialization: {e}")
        result.errors.append(f"Ошибка парсинга специальности: {str(e)}")

    # Итог
    if result.is_valid:
        logger.info("All fields parsed successfully:")
        logger.info(f"  FIO:            {result.full_name}")
        logger.info(f"  Direction:      {result.direction}")
        logger.info(f"  University:     {result.university}")
        logger.info(f"  Specialization: {result.specialization}")
    else:
        logger.warning(f"Missing fields: {result.missing_fields}")
        logger.warning(f"Errors: {result.errors}")

    return result