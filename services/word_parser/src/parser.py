import logging
from typing import List, Dict, Any
from sqlalchemy.orm import Session

from models import (
    University,
    Direction,
    Specialization,
    FormatControl,
    FormatRetests,
    StudyProgram,
    ControlTable,
)

logger = logging.getLogger(__name__)


class WordParser:
    """
    Парсер Word-документов с таблицами переаттестации.
    Работает напрямую с doc.tables (python-docx).
    """

    def __init__(self, session: Session):
        self.session = session
        self.stats = {
            "universities": 0,
            "directions": 0,
            "specializations": 0,
            "study_programs": 0,
            "control_tables": 0,
            "errors": [],
        }

    # ---------------------------------------------------
    # Универсальный get_or_create
    # ---------------------------------------------------
    def get_or_create(self, model, defaults=None, **kwargs):
        instance = self.session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance

        params = {**kwargs}
        if defaults:
            params.update(defaults)

        instance = model(**params)
        self.session.add(instance)
        self.session.flush()

        if model.__name__ == "University":
            self.stats["universities"] += 1
        elif model.__name__ == "Direction":
            self.stats["directions"] += 1
        elif model.__name__ == "Specialization":
            self.stats["specializations"] += 1
        elif model.__name__ == "StudyProgram":
            self.stats["study_programs"] += 1

        return instance

    # ---------------------------------------------------
    # Извлечение блоков (университет + специальность)
    # ---------------------------------------------------
    def extract_blocks(self, paragraphs: List[str]) -> List[Dict[str, str]]:
        blocks = []

        for line in paragraphs:
            if "прослушанных в" in line and "по специальности" in line:
                try:
                    part1 = line.split("прослушанных в")[1]
                    university = part1.split("по специальности")[0].strip()

                    speciality = line.split("по специальности")[1]
                    speciality = speciality.replace(":", "").strip()
                    speciality = speciality.replace("«", "").replace("»", "")

                    blocks.append(
                        {
                            "university": university,
                            "speciality": speciality,
                        }
                    )
                except Exception as e:
                    logger.error(f"Ошибка разбора заголовка: {e}")
                    self.stats["errors"].append(str(e))

        return blocks

    # ---------------------------------------------------
    # Основной метод
    # ---------------------------------------------------
    def parse_document(
        self,
        paragraphs: List[str],
        tables: List[List[List[str]]],
    ) -> Dict[str, Any]:

        blocks = self.extract_blocks(paragraphs)

        if not blocks:
            logger.warning("Не найдено ни одного блока 'прослушанных в'")
            return self.stats

        logger.info(f"Найдено блоков: {len(blocks)}")
        logger.info(f"Найдено таблиц: {len(tables)}")

        for index, block in enumerate(blocks):
            try:
                university = self.get_or_create(
                    University, name=block["university"]
                )

                direction = self.get_or_create(
                    Direction, name=block["speciality"]
                )

                specialization = self.get_or_create(
                    Specialization,
                    name=block["speciality"],
                    code="",
                    direction_id=direction.id,
                    university_id=university.id,
                )

                if index >= len(tables):
                    continue

                table = tables[index]

                # пропускаем шапку
                for row in table[1:]:
                    if len(row) < 7:
                        continue

                    self.save_control_record(specialization, row)

                self.session.commit()

            except Exception as e:
                logger.error(f"Ошибка обработки блока: {e}")
                self.stats["errors"].append(str(e))
                self.session.rollback()

        return self.stats

    # ---------------------------------------------------
    # Сохранение строки таблицы
    # ---------------------------------------------------
    def save_control_record(
        self,
        specialization: Specialization,
        row: List[str],
    ):
        try:
            discipline = row[0]
            hours_norm = row[1]
            form_norm = row[2]
            hours_fact = row[3]
            form_fact = row[4]
            hours_retest = row[5]
            form_retest = row[6]

            if not discipline:
                return

            program = self.get_or_create(
                StudyProgram, name=discipline
            )

            fc_norm = self.get_or_create(
                FormatControl, format_name=form_norm or "не указано"
            )

            fc_fact = self.get_or_create(
                FormatControl, format_name=form_fact or "не указано"
            )

            fret = self.get_or_create(
                FormatRetests, format_name=form_retest or "не указано"
            )

            control = ControlTable(
                specialization_id=specialization.id,
                study_program_id=program.id,
                format_control_norma_id=fc_norm.id,
                format_control_fact_id=fc_fact.id,
                format_retests_id=fret.id,
                hours_fact=hours_fact,
                hours_normal=hours_norm,
            )

            self.session.add(control)
            self.stats["control_tables"] += 1

        except Exception as e:
            logger.error(f"Ошибка сохранения строки: {e}")
            self.stats["errors"].append(str(e))