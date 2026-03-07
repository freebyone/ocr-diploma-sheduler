from pydantic import BaseModel
from typing import List, Optional


# ── Ответы для списка направлений ──

class DirectionListItem(BaseModel):
    id: int
    name: str
    is_used: bool
    student_count: int

    class Config:
        from_attributes = True


class DirectionsListResponse(BaseModel):
    directions: List[DirectionListItem]
    total: int


# ── Запрос на генерацию ──

class GenerateRequest(BaseModel):
    direction_ids: List[int]


class GeneratedFileInfo(BaseModel):
    direction_id: int
    direction_name: str
    filename: str
    success: bool
    error: Optional[str] = None


class GenerateResponse(BaseModel):
    generated: List[GeneratedFileInfo]
    total_success: int
    total_errors: int


# ── Детали направления ──

class StudentInfo(BaseModel):
    id: int
    full_name: str
    specialization_name: Optional[str] = None
    university_name: Optional[str] = None

    class Config:
        from_attributes = True


class ControlTableRow(BaseModel):
    id: int
    program_name: Optional[str] = None
    hours_normal: Optional[str] = None
    hours_fact: Optional[str] = None
    format_control_norma: Optional[str] = None
    format_control_fact: Optional[str] = None
    format_retests: Optional[str] = None

    class Config:
        from_attributes = True


class DirectionDetailResponse(BaseModel):
    id: int
    name: str
    is_used: bool
    students: List[StudentInfo]
    control_table: List[ControlTableRow]


# ── Общий ответ ──

class MessageResponse(BaseModel):
    message: str
    detail: Optional[str] = None


# ── Здоровье ──

class HealthResponse(BaseModel):
    status: str
    database: str
    version: str