import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, Float, Date, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://sris:sris_password@localhost:5432/sris_db",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class WeatherData(Base):
    """
    Satu baris = data curah hujan satu hari untuk Surabaya.
    Feature rainfall_7d, rainfall_30d, rainfall_90d dipakai di:
    - Phase 8: Road Health Index (bobot 0.2)
    - Phase 7: Feature Engineering (join ke tabel utama)
    """
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, unique=True, index=True)

    # Data mentah dari Open-Meteo
    precipitation_sum = Column(Float, nullable=True)   # mm/hari
    rain_sum = Column(Float, nullable=True)            # mm/hari
    precipitation_hours = Column(Float, nullable=True) # jam hujan per hari

    # Feature engineering - rolling sum
    rainfall_7d = Column(Float, nullable=True)   # total hujan 7 hari terakhir
    rainfall_30d = Column(Float, nullable=True)  # total hujan 30 hari terakhir
    rainfall_90d = Column(Float, nullable=True)  # total hujan 90 hari terakhir

    created_at = Column(DateTime, server_default=func.now())

    def to_dict(self) -> dict:
        return {
            "date": str(self.date),
            "precipitation_sum": self.precipitation_sum,
            "rain_sum": self.rain_sum,
            "precipitation_hours": self.precipitation_hours,
            "rainfall_7d": self.rainfall_7d,
            "rainfall_30d": self.rainfall_30d,
            "rainfall_90d": self.rainfall_90d,
        }


def ensure_tables():
    Base.metadata.create_all(bind=engine)
