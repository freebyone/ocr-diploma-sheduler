import os
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func

from database import get_db
from models import (
    IncomingDirection, Student, Specialization,
    ControlTable,
)
from schemas import (
    DirectionListItem, DirectionsListResponse,
    GenerateRequest, GenerateResponse, GeneratedFileInfo,
    DirectionDetailResponse, StudentInfo, ControlTableRow,
    MessageResponse, HealthResponse,
)
from word_generator import generate_order_for_direction
from config import settings

router = APIRouter()


# ══════════════════════════════════════════════
#  HEALTH CHECK
# ══════════════════════════════════════════════

@router.get("/health", response_model=HealthResponse, tags=["system"])
def health_check(db: Session = Depends(get_db)):
    """Проверка здоровья сервиса"""
    try:
        db.execute(func.now())
        db_status = "connected"
    except Exception:
        db_status = "disconnected"

    return HealthResponse(
        status="ok",
        database=db_status,
        version="1.0.0",
    )


# ══════════════════════════════════════════════
#  СПИСОК НАПРАВЛЕНИЙ
# ══════════════════════════════════════════════

@router.get(
    "/directions",
    response_model=DirectionsListResponse,
    tags=["directions"],
)
def list_directions(
    only_new: bool = Query(False, description="Только ещё не сгенерированные"),
    db: Session = Depends(get_db),
):
    """
    Получить список направлений.
    `only_new=true` — только те, по которым ещё не генерировался приказ.
    """
    query = db.query(IncomingDirection)

    if only_new:
        query = query.filter(IncomingDirection.is_used == False)

    directions = query.order_by(IncomingDirection.id).all()

    items: List[DirectionListItem] = []
    for d in directions:
        student_count = (
            db.query(func.count(Student.id))
            .filter(Student.incoming_direction_id == d.id)
            .scalar()
        )
        items.append(DirectionListItem(
            id=d.id,
            name=d.name,
            is_used=d.is_used,
            student_count=student_count or 0,
        ))

    return DirectionsListResponse(directions=items, total=len(items))


# ══════════════════════════════════════════════
#  ДЕТАЛИ НАПРАВЛЕНИЯ
# ══════════════════════════════════════════════

@router.get(
    "/directions/{direction_id}",
    response_model=DirectionDetailResponse,
    tags=["directions"],
)
def get_direction_detail(direction_id: int, db: Session = Depends(get_db)):
    """Детальная информация о направлении: студенты + таблица дисциплин"""
    direction = db.query(IncomingDirection).get(direction_id)
    if not direction:
        raise HTTPException(status_code=404, detail="Направление не найдено")

    # Студенты
    students = (
        db.query(Student)
        .filter(Student.incoming_direction_id == direction_id)
        .options(
            joinedload(Student.specialization)
            .joinedload(Specialization.university)
        )
        .order_by(Student.full_name)
        .all()
    )

    student_infos = []
    for s in students:
        spec_name = s.specialization.name if s.specialization else None
        uni_name = (
            s.specialization.university.name
            if s.specialization and s.specialization.university
            else None
        )
        student_infos.append(StudentInfo(
            id=s.id,
            full_name=s.full_name,
            specialization_name=spec_name,
            university_name=uni_name,
        ))

    # Таблица контроля
    control_rows = (
        db.query(ControlTable)
        .filter(ControlTable.incoming_direction_id == direction_id)
        .options(
            joinedload(ControlTable.study_program),
            joinedload(ControlTable.format_control_norma),
            joinedload(ControlTable.format_control_fact),
            joinedload(ControlTable.format_retests),
        )
        .all()
    )

    # Дедупликация
    seen = set()
    unique_rows = []
    for ct in control_rows:
        if ct.study_program_id not in seen:
            seen.add(ct.study_program_id)
            unique_rows.append(ct)

    control_infos = []
    for ct in unique_rows:
        control_infos.append(ControlTableRow(
            id=ct.id,
            program_name=ct.study_program.name if ct.study_program else None,
            hours_normal=ct.hours_normal,
            hours_fact=ct.hours_fact,
            format_control_norma=(
                ct.format_control_norma.format_name
                if ct.format_control_norma else None
            ),
            format_control_fact=(
                ct.format_control_fact.format_name
                if ct.format_control_fact else None
            ),
            format_retests=(
                ct.format_retests.format_name
                if ct.format_retests else None
            ),
        ))

    return DirectionDetailResponse(
        id=direction.id,
        name=direction.name,
        is_used=direction.is_used,
        students=student_infos,
        control_table=control_infos,
    )


# ══════════════════════════════════════════════
#  ГЕНЕРАЦИЯ ПРИКАЗОВ
# ══════════════════════════════════════════════

@router.post(
    "/generate",
    response_model=GenerateResponse,
    tags=["generation"],
)
def generate_orders(request: GenerateRequest, db: Session = Depends(get_db)):
    """
    Сгенерировать Word-приказы по выбранным направлениям.
    Принимает список `direction_ids`.
    """
    if not request.direction_ids:
        raise HTTPException(
            status_code=400,
            detail="Список direction_ids пуст"
        )

    directions = (
        db.query(IncomingDirection)
        .filter(IncomingDirection.id.in_(request.direction_ids))
        .all()
    )

    if not directions:
        raise HTTPException(
            status_code=404,
            detail="Направления не найдены"
        )

    results: List[GeneratedFileInfo] = []

    for direction in directions:
        try:
            filepath = generate_order_for_direction(
                session=db,
                direction=direction,
                output_dir=settings.OUTPUT_DIR,
            )

            # Помечаем как обработанное
            direction.is_used = True
            db.commit()

            filename = os.path.basename(filepath)
            results.append(GeneratedFileInfo(
                direction_id=direction.id,
                direction_name=direction.name,
                filename=filename,
                success=True,
            ))

        except Exception as e:
            db.rollback()
            results.append(GeneratedFileInfo(
                direction_id=direction.id,
                direction_name=direction.name,
                filename="",
                success=False,
                error=str(e),
            ))

    total_success = sum(1 for r in results if r.success)
    total_errors = sum(1 for r in results if not r.success)

    return GenerateResponse(
        generated=results,
        total_success=total_success,
        total_errors=total_errors,
    )


# ══════════════════════════════════════════════
#  ГЕНЕРАЦИЯ ВСЕХ НОВЫХ
# ══════════════════════════════════════════════

@router.post(
    "/generate-all",
    response_model=GenerateResponse,
    tags=["generation"],
)
def generate_all_new_orders(db: Session = Depends(get_db)):
    """Сгенерировать приказы для всех направлений, где is_used=False"""
    directions = (
        db.query(IncomingDirection)
        .filter(IncomingDirection.is_used == False)
        .all()
    )

    if not directions:
        return GenerateResponse(
            generated=[],
            total_success=0,
            total_errors=0,
        )

    # Собираем id и переиспользуем generate_orders
    request = GenerateRequest(
        direction_ids=[d.id for d in directions]
    )
    return generate_orders(request, db)


# ══════════════════════════════════════════════
#  СКАЧИВАНИЕ ФАЙЛА
# ══════════════════════════════════════════════

@router.get(
    "/download/{filename}",
    tags=["files"],
)
def download_file(filename: str):
    """Скачать сгенерированный Word-файл"""
    filepath = os.path.join(settings.OUTPUT_DIR, filename)

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Файл не найден")

    return FileResponse(
        path=filepath,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


# ══════════════════════════════════════════════
#  СПИСОК СГЕНЕРИРОВАННЫХ ФАЙЛОВ
# ══════════════════════════════════════════════

@router.get(
    "/files",
    tags=["files"],
)
def list_generated_files():
    """Список всех сгенерированных файлов"""
    output_dir = settings.OUTPUT_DIR

    if not os.path.exists(output_dir):
        return {"files": []}

    files = []
    for fname in sorted(os.listdir(output_dir)):
        if fname.endswith('.docx'):
            fpath = os.path.join(output_dir, fname)
            stat = os.stat(fpath)
            files.append({
                "filename": fname,
                "size_bytes": stat.st_size,
                "size_kb": round(stat.st_size / 1024, 1),
                "download_url": f"/api/download/{fname}",
            })

    return {"files": files, "total": len(files)}


# ══════════════════════════════════════════════
#  СБРОС ФЛАГА is_used
# ══════════════════════════════════════════════

@router.post(
    "/directions/{direction_id}/reset",
    response_model=MessageResponse,
    tags=["directions"],
)
def reset_direction(direction_id: int, db: Session = Depends(get_db)):
    """Сбросить is_used, чтобы можно было перегенерировать"""
    direction = db.query(IncomingDirection).get(direction_id)
    if not direction:
        raise HTTPException(status_code=404, detail="Направление не найдено")

    direction.is_used = False
    db.commit()

    return MessageResponse(
        message="Флаг сброшен",
        detail=f"Направление '{direction.name}' можно перегенерировать"
    )