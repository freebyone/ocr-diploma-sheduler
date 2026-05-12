from sqlalchemy import Column, Integer, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class PairCounter(Base):
    __tablename__ = "pair_counter"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pair_index = Column(Integer, nullable=False, comment="Номер пары файлов (prefix)")
    surname = Column(String(255), nullable=False, comment="Фамилия из имени файла")
    xlsx_filename = Column(String(500), nullable=True, comment="Имя xlsx файла")
    pdf_filename = Column(String(500), nullable=True, comment="Имя pdf файла")
    minio_xlsx_key = Column(String(500), nullable=True, comment="Ключ в MinIO для xlsx")
    minio_jpg_key = Column(String(500), nullable=True, comment="Ключ в MinIO для jpg")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __repr__(self):
        return (
            f"<PairCounter(id={self.id}, pair_index={self.pair_index}, "
            f"surname={self.surname})>"
        )