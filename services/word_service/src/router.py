# router.py — исправленная версия

import os
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func

from database import get_db
from models import IncomingDirection, Student, Specialization, ControlTable
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
#  Важно: маршрут /health здесь, но app включает
#  router с prefix="/api", поэтому реальный путь
#  будет /api/health. Если нужен /health — добавить
#  отдельный маршрут прямо в main.py.
# ══════════════════════════════════════════════

@router.get("/health", response_model=HealthResponse, tags=["system"])
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(func.now())
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    return HealthResponse(status="ok", database=db_status, version="1.0.0")


# ══════════════════════════════════════════════
#  СПИСОК НАПРАВЛЕНИЙ
# ══════════════════════════════════════════════

@router.get("/directions", response_model=DirectionsListResponse, tags=["directions"])
def list_directions(
    only_new: bool = Query(False, description="Только ещё не сгенерированные"),
    db: Session = Depends(get_db),
):
    query = db.query(IncomingDirection)
    if only_new:
        query = query.filter(IncomingDirection.is_used == False)  # noqa: E712

    directions = query.order_by(IncomingDirection.id).all()

    items: List[DirectionListItem] = []
    for d in directions:
        count = (
            db.query(func.count(Student.id))
            .filter(Student.incoming_direction_id == d.id)
            .scalar()
        ) or 0
        items.append(DirectionListItem(
            id=d.id,
            name=d.name,
            is_used=bool(d.is_used),
            student_count=count,
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
    # .get() устарел в SQLAlchemy 2.x → используем filter().first()
    direction = (
        db.query(IncomingDirection)
        .filter(IncomingDirection.id == direction_id)
        .first()
    )
    if not direction:
        raise HTTPException(status_code=404, detail="Направление не найдено")

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

    student_infos = [
        StudentInfo(
            id=s.id,
            full_name=s.full_name,
            specialization_name=(
                s.specialization.name if s.specialization else None
            ),
            university_name=(
                s.specialization.university.name
                if s.specialization and s.specialization.university
                else None
            ),
        )
        for s in students
    ]

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

    seen: set = set()
    control_infos: List[ControlTableRow] = []
    for ct in control_rows:
        if ct.study_program_id not in seen:
            seen.add(ct.study_program_id)
            control_infos.append(ControlTableRow(
                id=ct.id,
                program_name=(
                    ct.study_program.name if ct.study_program else None
                ),
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
        is_used=bool(direction.is_used),
        students=student_infos,
        control_table=control_infos,
    )


# ══════════════════════════════════════════════
#  ГЕНЕРАЦИЯ ПРИКАЗОВ
# ══════════════════════════════════════════════

@router.post("/generate", response_model=GenerateResponse, tags=["generation"])
def generate_orders(request: GenerateRequest, db: Session = Depends(get_db)):
    if not request.direction_ids:
        raise HTTPException(status_code=400, detail="Список direction_ids пуст")

    directions = (
        db.query(IncomingDirection)
        .filter(IncomingDirection.id.in_(request.direction_ids))
        .all()
    )

    if not directions:
        raise HTTPException(status_code=404, detail="Направления не найдены")

    results: List[GeneratedFileInfo] = []

    for direction in directions:
        try:
            filepath = generate_order_for_direction(
                session=db,
                direction=direction,
                output_dir=settings.OUTPUT_DIR,
            )
            direction.is_used = True
            db.commit()

            results.append(GeneratedFileInfo(
                direction_id=direction.id,
                direction_name=direction.name,
                filename=os.path.basename(filepath),
                success=True,
            ))
        except Exception as exc:
            db.rollback()
            results.append(GeneratedFileInfo(
                direction_id=direction.id,
                direction_name=direction.name,
                filename="",
                success=False,
                error=str(exc),
            ))

    return GenerateResponse(
        generated=results,
        total_success=sum(1 for r in results if r.success),
        total_errors=sum(1 for r in results if not r.success),
    )


@router.post("/generate-all", response_model=GenerateResponse, tags=["generation"])
def generate_all_new_orders(db: Session = Depends(get_db)):
    """Сгенерировать приказы для всех направлений, где is_used=False."""
    directions = (
        db.query(IncomingDirection)
        .filter(IncomingDirection.is_used == False)  # noqa: E712
        .all()
    )

    if not directions:
        return GenerateResponse(generated=[], total_success=0, total_errors=0)

    return generate_orders(
        GenerateRequest(direction_ids=[d.id for d in directions]),
        db,
    )


# ══════════════════════════════════════════════
#  СКАЧИВАНИЕ / СПИСОК ФАЙЛОВ
# ══════════════════════════════════════════════

@router.get("/download/{filename}", tags=["files"])
def download_file(filename: str):
    # Защита от path-traversal
    if os.sep in filename or filename.startswith('.'):
        raise HTTPException(status_code=400, detail="Недопустимое имя файла")

    filepath = os.path.join(settings.OUTPUT_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Файл не найден")

    return FileResponse(
        path=filepath,
        filename=filename,
        media_type=(
            "application/vnd.openxmlformats-officedocument"
            ".wordprocessingml.document"
        ),
    )


@router.get("/files", tags=["files"])
def list_generated_files():
    output_dir = settings.OUTPUT_DIR
    if not os.path.exists(output_dir):
        return {"files": [], "total": 0}

    files = []
    for fname in sorted(os.listdir(output_dir)):
        if fname.endswith('.docx'):
            fpath = os.path.join(output_dir, fname)
            size  = os.stat(fpath).st_size
            files.append({
                "filename":     fname,
                "size_bytes":   size,
                "size_kb":      round(size / 1024, 1),
                "download_url": f"/api/download/{fname}",
            })

    return {"files": files, "total": len(files)}


# ══════════════════════════════════════════════
#  СБРОС ФЛАГА
# ══════════════════════════════════════════════

@router.post(
    "/directions/{direction_id}/reset",
    response_model=MessageResponse,
    tags=["directions"],
)
def reset_direction(direction_id: int, db: Session = Depends(get_db)):
    direction = (
        db.query(IncomingDirection)
        .filter(IncomingDirection.id == direction_id)
        .first()
    )
    if not direction:
        raise HTTPException(status_code=404, detail="Направление не найдено")

    direction.is_used = False
    db.commit()

    return MessageResponse(
        message="Флаг сброшен",
        detail=f"Направление «{direction.name}» можно перегенерировать",
    )


@router.delete("/files/{filename}", response_model=MessageResponse, tags=["files"])
def delete_file(filename: str):
    """Удалить один сгенерированный файл"""
    if os.sep in filename or filename.startswith('.'):
        raise HTTPException(status_code=400, detail="Недопустимое имя файла")

    filepath = os.path.join(settings.OUTPUT_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Файл не найден")

    os.remove(filepath)
    return MessageResponse(
        message="Файл удалён",
        detail=f"Удалён: {filename}",
    )


@router.delete("/files", response_model=MessageResponse, tags=["files"])
def delete_all_files():
    """Удалить все сгенерированные файлы"""
    output_dir = settings.OUTPUT_DIR
    if not os.path.exists(output_dir):
        return MessageResponse(message="Нет файлов для удаления")

    deleted = 0
    for fname in os.listdir(output_dir):
        if fname.endswith('.docx'):
            os.remove(os.path.join(output_dir, fname))
            deleted += 1

    return MessageResponse(
        message=f"Удалено файлов: {deleted}",
        detail=f"Очищена директория {output_dir}",
    )