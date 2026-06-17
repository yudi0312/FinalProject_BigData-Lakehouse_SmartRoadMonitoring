import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from .database import Base, engine, get_db
from .models import Report
from .schemas import PredictionResponse, ReportCreateResponse, ReportResponse, StatsResponse
from .services.yolo_service import DamagePrediction, YoloInferenceError, load_model, predict_damage


UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", Path(__file__).resolve().parents[1] / "uploads"))
UPLOAD_DIR.mkdir(exist_ok=True)

app = FastAPI(
    title="SRIS Crowdsourcing API",
    description="API untuk laporan jalan rusak Kota Surabaya.",
    version="1.0.0",
)

origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)
    load_model()


def next_report_id(db: Session) -> str:
    total = db.scalar(select(func.count()).select_from(Report)) or 0
    return f"R{total + 1:03d}"


def ensure_image(file: UploadFile) -> None:
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail=f"{file.filename} bukan file gambar.")


def save_upload(file: UploadFile, report_id: str) -> tuple[str, Path]:
    ensure_image(file)
    suffix = Path(file.filename or "photo.jpg").suffix.lower() or ".jpg"
    filename = f"{report_id}_{uuid4().hex}{suffix}"
    target = UPLOAD_DIR / filename
    with target.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return f"/uploads/{filename}", target


def best_prediction_for_images(image_paths: list[Path]) -> DamagePrediction:
    best_prediction: DamagePrediction | None = None
    for image_path in image_paths:
        prediction = predict_damage(image_path)
        if best_prediction is None or prediction.confidence > best_prediction.confidence:
            best_prediction = prediction

    return best_prediction or DamagePrediction(
        damage_type="Unknown",
        confidence=0,
        severity_score=0,
    )


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/predict", response_model=PredictionResponse)
async def predict(file: UploadFile = File(...)) -> PredictionResponse:
    ensure_image(file)
    _, image_path = save_upload(file, "PREDICT")
    try:
        return predict_damage(image_path)
    except YoloInferenceError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/reports", response_model=ReportCreateResponse, status_code=201)
async def create_report(
    reporter_name: str = Form(...),
    email: str | None = Form(None),
    road_name: str = Form(...),
    district: str = Form(...),
    village: str = Form(...),
    description: str = Form(...),
    latitude: float = Form(...),
    longitude: float = Form(...),
    photos: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
) -> ReportCreateResponse:
    if len(photos) > 5:
        raise HTTPException(status_code=400, detail="Upload foto maksimal 5 file.")

    report_id = next_report_id(db)
    saved_uploads = [save_upload(photo, report_id) for photo in photos]
    image_urls = [upload[0] for upload in saved_uploads]
    local_image_paths = [upload[1] for upload in saved_uploads]

    try:
        prediction = best_prediction_for_images(local_image_paths)
    except YoloInferenceError as exc:
        raise HTTPException(status_code=503, detail=f"AI inference failed: {exc}") from exc

    report = Report(
        report_id=report_id,
        reporter_name=reporter_name,
        email=email,
        road_name=road_name,
        district=district,
        village=village,
        description=description,
        latitude=latitude,
        longitude=longitude,
        image_path=",".join(image_urls),
        damage_type=prediction.damage_type,
        severity_score=prediction.severity_score,
        confidence=prediction.confidence,
        status="Pending",
    )
    db.add(report)
    db.commit()
    db.refresh(report)

    return ReportCreateResponse(
        report_id=report.report_id,
        damage_type=report.damage_type,
        confidence=report.confidence,
        severity_score=report.severity_score,
    )


@app.get("/reports", response_model=list[ReportResponse])
def list_reports(db: Session = Depends(get_db)) -> list[Report]:
    return list(db.scalars(select(Report).order_by(desc(Report.report_date))).all())


@app.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)) -> StatsResponse:
    total = db.scalar(select(func.count()).select_from(Report)) or 0
    today = datetime.now(timezone.utc).date()
    today_count = (
        db.scalar(
            select(func.count())
            .select_from(Report)
            .where(func.date(Report.report_date) == today)
        )
        or 0
    )
    top_district_row = db.execute(
        select(Report.district, func.count(Report.report_id).label("total"))
        .group_by(Report.district)
        .order_by(desc("total"))
        .limit(1)
    ).first()
    detected_damage = (
        db.scalar(
            select(func.count())
            .select_from(Report)
            .where(Report.damage_type.is_not(None))
            .where(Report.damage_type != "Unknown")
        )
        or 0
    )
    average_severity = (
        db.scalar(
            select(func.coalesce(func.avg(Report.severity_score), 0))
            .where(Report.severity_score.is_not(None))
        )
        or 0
    )
    pothole_count = (
        db.scalar(
            select(func.count())
            .select_from(Report)
            .where(Report.damage_type == "D40_Pothole")
        )
        or 0
    )
    crack_count = (
        db.scalar(
            select(func.count())
            .select_from(Report)
            .where(
                Report.damage_type.in_(
                    [
                        "D00_Longitudinal_Crack",
                        "D10_Transverse_Crack",
                        "D20_Alligator_Crack",
                    ]
                )
            )
        )
        or 0
    )

    return StatsResponse(
        total_reports=total,
        today_reports=today_count,
        top_district=top_district_row[0] if top_district_row else None,
        total_detected_damage=detected_damage,
        average_severity_score=round(float(average_severity), 2),
        pothole_count=pothole_count,
        crack_count=crack_count,
    )
