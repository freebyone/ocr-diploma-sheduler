from langchain_gigachat.chat_models import GigaChat
from pydantic import BaseModel, Field
import logging
# from parser_service.config import service_config
logging.basicConfig(
    level=logging.INFO,  # Уровень: DEBUG, INFO, WARNING, ERROR
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class StudentInfo(BaseModel):
    full_name: str = Field(description="ФИО студента. Имя фамилия и отчество при наличии вместе через пробел")
    direction: str = Field(description="Это квалификация студента")
    specialization: str = Field(description="Это специальность на которой учился студент")
    university: str = Field(description="Название университета")

class ParsedStudent(StudentInfo):
    code: str
    

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
        try:
            logger.info("Structing LLM")
            structed_llm = self._model.with_structured_output(StudentInfo, method="json_mode",include_raw=True)
        except Exception as e:
            logger.error(f"Error in structing LLM: {e}")
        try:
            logger.info("Invoker LLM")
            response = structed_llm.invoke(input_prompt)
            return response
        except Exception as e:
            logger.error(f"Error in answer LLM: {e}")

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

if __name__ == "__main__":
    model = GigaChat(
        model="GigaChat-2",
        credentials="",
        verify_ssl_certs=False,
        top_p=0,
        temperature=0.1
    )
    # test = """
    # |ref|>text<|/ref|><|det|>[[341, 101, 781, 121]]<|/det|>\n1. СВЕДЕНИЯ О ЛИЧНОСТИ ОБЛАДАТЕЛЯ ДИПЛОМА \n\n<|ref|>text<|/ref|><|det|>[[338, 180, 417, 196]]<|/det|>\nФамилия \n\n<|ref|>text<|/ref|><|det|>[[522, 183, 617, 198]]<|/det|>\nАнисимова \n\n<|ref|>text<|/ref|><|det|>[[54, 231, 311, 357]]<|/det|>\nРОССИЙСКАЯ\nФЕДЕРАЦИЯ\nАвтономная некоммерческая\nорганизация профессионального\nобразования\n«Гуманитарно-технический\nколледж «Знание»\nг.о. Подольск \n\n<|ref|>text<|/ref|><|det|>[[338, 245, 380, 260]]<|/det|>\nИмя \n\n<|ref|>text<|/ref|><|det|>[[522, 250, 601, 266]]<|/det|>\nВиктория \n\n<|ref|>text<|/ref|><|det|>[[338, 310, 640, 329]]<|/det|>\nОтчество (при наличии) Владимировна \n\n<|ref|>text<|/ref|><|det|>[[335, 375, 654, 394]]<|/det|>\nДата рождения 27 мая 2005 года \n\n<|ref|>text<|/ref|><|det|>[[333, 440, 768, 470]]<|/det|>\nПредыдущий документ об образовании или об образовании\nи о квалификации \n\n<|ref|>text<|/ref|><|det|>[[448, 477, 860, 496]]<|/det|>\nаттестат об основном общем образовании, 2021 год \n\n<|ref|>text<|/ref|><|det|>[[108, 526, 238, 556]]<|/det|>\nПРИЛОЖЕНИЕ\nК ДИПЛОМУ \n\n<|ref|>text<|/ref|><|det|>[[53, 562, 291, 593]]<|/det|>\nо среднем профессиональном\nобразовании \n\n<|ref|>text<|/ref|><|det|>[[328, 565, 750, 610]]<|/det|>\n2. СВЕДЕНИЯ ОБ ОБРАЗОВАТЕЛЬНОЙ ПРОГРАММЕ\nСРЕДНЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ\nИ О КВАЛИФИКАЦИИ \n\n<|ref|>text<|/ref|><|det|>[[65, 620, 279, 638]]<|/det|>\n135024 1747598 \n\n<|ref|>text<|/ref|><|det|>[[104, 660, 238, 709]]<|/det|>\nРегистрационный\nномер\n2025 \n\n<|ref|>text<|/ref|><|det|>[[327, 653, 734, 684]]<|/det|>\nСрок освоения образовательной программы по очной\nформе обучения \n\n<|ref|>text<|/ref|><|det|>[[327, 688, 472, 706]]<|/det|>\n3 года 10 месяцев \n\n<|ref|>text<|/ref|><|det|>[[327, 754, 438, 770]]<|/det|>\nКвалификация \n\n<|ref|>text<|/ref|><|det|>[[117, 777, 217, 792]]<|/det|>\nДата выдачи \n\n<|ref|>text<|/ref|><|det|>[[90, 799, 240, 815]]<|/det|>\n30 июня 2025 года \n\n<|ref|>text<|/ref|><|det|>[[321, 781, 406, 798]]<|/det|>\nДизайнер \n\n<|ref|>text<|/ref|><|det|>[[321, 811, 348, 825]]<|/det|>\nпо \n\n<|ref|>text<|/ref|><|det|>[[520, 823, 768, 856]]<|/det|>\nспециальности\n54.02.01 Дизайн (по отраслям) \n\n<|ref|>text<|/ref|><|det|>[[240, 875, 387, 912]]<|/det|>\nКСП ВЕРНА\nФИШК \n\n<|ref|>text<|/ref|><|det|>[[439, 895, 597, 921]]<|/det|>\nШУСТРОВА М.В \n\n<|ref|>image<|/ref|><|det|>[[303, 917, 411, 990]]<|/det|>
    # """

    test = """      "ocr_text": "1. СВЕДЕНИЯ О ЛИЧНОСТИ ОБЛАДАТЕЛЯ ДИПЛОМА\n\nФамилия    Анисимова\n\nРОССИЙСКАЯ ФЕДЕРАЦИЯ    Имя    Виктория\nАвтономная некоммерческая организация профессионального образования\n«Гуманитарно-технический колледж «Знание»\nг.о. Подольск\n\nОтчество (при наличии) Владимировна\n\nДата рождения    27 мая 2005 года\n\nПредыдущий документ об образовании или об образовании и о квалификации\nаттестат об основном общем образовании, 2021 год\n\nПРИЛОЖЕНИЕ К ДИПЛОМУ\nо среднем профессиональном образовании\n\n2. СВЕДЕНИЯ ОБ ОБРАЗОВАТЕЛЬНОЙ ПРОГРАММЕ СРЕДНЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ И О КВАЛИФИКАЦИИ\n\n135024 1747598\n\nРегистрационный номер\n2025\n\nСрок освоения образовательной программы по очной форме обучения\n3 года 10 месяцев\n\nКвалификация\nДизайнер\nпо\nспециальности\n54.02.01 Дизайн (по отраслям)\n\nДата выдачи\n30 июня 2025 года\n\nКПП ВЕРНА\nФИО\n\nШУСТРОВА М.В.","""
    parser = LLMParser(model)
    res = parser.parse_image_text(test)
    print(f"TOKENS: {res['raw'].response_metadata["token_usage"]["total_tokens"]}")
    print('\n',res["parsed"].full_name)
    print('\n',res["parsed"].direction)
    print('\n',res["parsed"].university)
    splited = LLMParser.split_code(res["parsed"].specialization)
    if isinstance(splited, dict):
        print('\n',splited["code"])
        print('\n',splited["name"])
    else:
        print('\n',res["parsed"].specialization)


        
        