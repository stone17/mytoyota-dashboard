# app/database.py
import datetime
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import inspect, text

from .config import DATA_DIR

DB_FILE = DATA_DIR / "mytoyota.db"
DATABASE_URL = f"sqlite:///{DB_FILE.resolve()}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
_LOGGER = logging.getLogger(__name__)

class VehicleReading(Base):
    __tablename__ = "readings"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    vin = Column(String, index=True)
    odometer = Column(Float)
    fuel_level = Column(Float)
    total_range = Column(Float)
    daily_distance = Column(Float)
    daily_fuel_consumed = Column(Float)


class Trip(Base):
    __tablename__ = "trips"

    id = Column(Integer, primary_key=True, index=True)
    vin = Column(String, index=True)
    start_timestamp = Column(DateTime, index=True)
    end_timestamp = Column(DateTime)
    start_address = Column(String)
    start_lat = Column(Float, nullable=True)
    start_lon = Column(Float, nullable=True)
    end_address = Column(String)
    end_lat = Column(Float, nullable=True)
    end_lon = Column(Float, nullable=True)
    distance_km = Column(Float)
    fuel_consumption_l_100km = Column(Float)
    # New columns for detailed trip data
    duration_seconds = Column(Integer, nullable=True)
    average_speed_kmh = Column(Float, nullable=True)
    max_speed_kmh = Column(Float, nullable=True)
    ev_distance_km = Column(Float, nullable=True)
    ev_duration_seconds = Column(Integer, nullable=True)
    score_acceleration = Column(Integer, nullable=True)
    score_braking = Column(Integer, nullable=True)
    score_global = Column(Integer, nullable=True)

    # --- New columns for pre-calculated imperial units ---
    distance_mi = Column(Float, nullable=True)
    mpg = Column(Float, nullable=True)
    mpg_uk = Column(Float, nullable=True)
    average_speed_mph = Column(Float, nullable=True)
    ev_distance_mi = Column(Float, nullable=True)

def _add_missing_columns(engine):
    """Checks for and adds missing columns to the trips table without dropping data."""
    inspector = inspect(engine)
    try:
        columns = [c['name'] for c in inspector.get_columns('trips')]
    except Exception:
        # The table probably doesn't exist yet, let create_all handle it.
        return
    
    new_columns = {
        'distance_mi': 'FLOAT',
        'mpg': 'FLOAT',
        'mpg_uk': 'FLOAT',
        'average_speed_mph': 'FLOAT',
        'ev_distance_mi': 'FLOAT'
    }

    with engine.connect() as connection:
        for col_name, col_type in new_columns.items():
            if col_name not in columns:
                _LOGGER.info(f"Adding missing column '{col_name}' to 'trips' table.")
                connection.execute(text(f'ALTER TABLE trips ADD COLUMN {col_name} {col_type}'))
        connection.commit()

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    DATA_DIR.mkdir(exist_ok=True)
    Base.metadata.create_all(bind=engine)
    # Run the migration to add missing columns after ensuring tables exist
    _add_missing_columns(engine)

def get_latest_reading(vin: str) -> VehicleReading | None:
    """Gets the most recent database reading for a given VIN."""
    db = SessionLocal()
    try:
        latest = db.query(VehicleReading).filter(
            VehicleReading.vin == vin
        ).order_by(VehicleReading.timestamp.desc()).first()
        return latest
    finally:
        db.close()

def add_reading(vehicle_data: dict):
    """Adds a new vehicle reading to the database."""
    db = SessionLocal()
    try:
        daily_stats = vehicle_data.get("statistics", {}).get("daily") or {}
        reading = VehicleReading(
            vin=vehicle_data.get("vin"),
            odometer=vehicle_data.get("dashboard", {}).get("odometer"),
            fuel_level=vehicle_data.get("dashboard", {}).get("fuel_level"),
            total_range=vehicle_data.get("dashboard", {}).get("total_range"),
            daily_distance=daily_stats.get("distance"),
            daily_fuel_consumed=daily_stats.get("fuel_consumed")
        )
        # Only add if we have a VIN and odometer reading
        if reading.vin and reading.odometer is not None:
            db.add(reading)
            db.commit()
            db.refresh(reading)
    finally:
        db.close()