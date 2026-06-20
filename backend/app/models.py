from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class Report(Base):
    __tablename__ = "reports"

    report_id: Mapped[str] = mapped_column(String(12), primary_key=True)
    report_date: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    reporter_name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[str | None] = mapped_column(String(160))
    road_name: Mapped[str] = mapped_column(String(180), nullable=False)
    district: Mapped[str] = mapped_column(String(120), nullable=False)
    village: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    image_path: Mapped[str | None] = mapped_column(Text)
    damage_type: Mapped[str | None] = mapped_column(String(100))
    severity_score: Mapped[int | None] = mapped_column(Integer)
    confidence: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(40), default="Pending", nullable=False)


class RoadHealthIndex(Base):
    __tablename__ = "road_health_index"

    road_name: Mapped[str] = mapped_column(Text, primary_key=True)
    district: Mapped[str | None] = mapped_column(Text)
    severity: Mapped[float | None] = mapped_column(Float)
    rainfall: Mapped[float] = mapped_column(Float, nullable=False)
    traffic: Mapped[float] = mapped_column(Float, nullable=False)
    road_age_score: Mapped[float] = mapped_column(Float, nullable=False)
    road_health_index: Mapped[float | None] = mapped_column(Float)
    batch_time: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)


class PriorityScore(Base):
    __tablename__ = "priority_score"

    report_id: Mapped[str] = mapped_column(Text, primary_key=True)
    road_name: Mapped[str | None] = mapped_column(Text)
    district: Mapped[str | None] = mapped_column(Text)
    severity_score: Mapped[int | None] = mapped_column(Integer)
    traffic_score: Mapped[float] = mapped_column(Float, nullable=False)
    accident_score: Mapped[float] = mapped_column(Float, nullable=False)
    complaint_score: Mapped[float] = mapped_column(Float, nullable=False)
    news_score: Mapped[float] = mapped_column(Float, nullable=False)
    priority_score: Mapped[float | None] = mapped_column(Float)
    batch_time: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

