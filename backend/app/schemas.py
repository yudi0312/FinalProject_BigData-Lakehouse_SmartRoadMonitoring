from datetime import datetime

from pydantic import BaseModel


class PredictionResponse(BaseModel):
    damage_type: str
    severity_score: int
    confidence: float


class ReportCreateResponse(BaseModel):
    report_id: str
    damage_type: str | None
    confidence: float | None
    severity_score: int | None


class ReportResponse(BaseModel):
    report_id: str
    report_date: datetime
    reporter_name: str
    email: str | None
    road_name: str
    district: str
    village: str
    description: str
    latitude: float
    longitude: float
    image_path: str | None
    damage_type: str | None
    severity_score: int | None
    confidence: float | None
    status: str

    model_config = {"from_attributes": True}


class StatsResponse(BaseModel):
    total_reports: int
    today_reports: int
    top_district: str | None
    total_detected_damage: int
    average_severity_score: float
    pothole_count: int
    crack_count: int
