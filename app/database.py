# app/database.py
import datetime
import logging
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import inspect, text
from sqlalchemy.types import TypeDecorator, TEXT

from .config import config_manager, DATA_DIR

DB_FILE = DATA_DIR / config_manager.settings.get("database_filename", "mytoyota.db")
DATABASE_URL = f"sqlite:///{DB_FILE.resolve()}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
_LOGGER = logging.getLogger(__name__)


class SafeJSON(TypeDecorator):
    """
    Custom JSON type for SQLite to gracefully handle empty strings by treating
    them as NULL, preventing JSONDecodeError during data retrieval.
    """
    impl = TEXT
    cache_ok = True

    def process_result_value(self, value, dialect):
        """On the way out from the DB, load JSON from text."""
        if value is None or value == '':
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            _LOGGER.warning(f"Could not decode invalid JSON value from DB: {value}")
            return None

    def process_bind_param(self, value, dialect):
        """On the way into the DB, dump JSON to text."""
        if value is None:
            return None
        return json.dumps(value)


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
    start_address = Column(String, nullable=True)
    start_lat = Column(Float, nullable=True)
    start_lon = Column(Float, nullable=True)
    end_address = Column(String, nullable=True)
    end_lat = Column(Float, nullable=True)
    end_lon = Column(Float, nullable=True)
    distance_km = Column(Float, nullable=True)
    fuel_consumption_l_100km = Column(Float, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    average_speed_kmh = Column(Float, nullable=True)
    max_speed_kmh = Column(Float, nullable=True)
    ev_distance_km = Column(Float, nullable=True)
    ev_duration_seconds = Column(Integer, nullable=True)
    score_acceleration = Column(Integer, nullable=True)
    score_braking = Column(Integer, nullable=True)
    score_global = Column(Integer, nullable=True)
    api_id = Column(String, index=True, nullable=True)

    # --- New columns added ---
    countries = Column(SafeJSON, nullable=True)
    night_trip = Column(Boolean, nullable=True)
    length_overspeed_km = Column(Float, nullable=True)
    duration_overspeed_seconds = Column(Integer, nullable=True)
    length_highway_km = Column(Float, nullable=True)
    duration_highway_seconds = Column(Integer, nullable=True)
    score_advice = Column(Integer, nullable=True)
    score_constant_speed = Column(Integer, nullable=True)
    hdc_charge_duration_seconds = Column(Integer, nullable=True)
    hdc_charge_distance_km = Column(Float, nullable=True)
    hdc_eco_duration_seconds = Column(Integer, nullable=True)
    hdc_eco_distance_km = Column(Float, nullable=True)
    hdc_power_duration_seconds = Column(Integer, nullable=True)
    hdc_power_distance_km = Column(Float, nullable=True)

    # --- Columns for pre-calculated imperial units ---
    distance_mi = Column(Float, nullable=True)
    mpg = Column(Float, nullable=True)
    mpg_uk = Column(Float, nullable=True)
    average_speed_mph = Column(Float, nullable=True)
    ev_distance_mi = Column(Float, nullable=True)
    route = Column(SafeJSON, nullable=True)
    poll_id = Column(String, index=True, nullable=True)

class ServiceHistory(Base):
    __tablename__ = "service_history"

    id = Column(String, primary_key=True, index=True)
    vin = Column(String, index=True)
    service_date = Column(DateTime)
    mileage = Column(Float)
    unit = Column(String)
    service_provider = Column(String, nullable=True)
    service_category = Column(String, nullable=True)
    notes = Column(String, nullable=True)
    customer_created_record = Column(Boolean)
    ro_number = Column(String, nullable=True)
    operations_performed = Column(SafeJSON, nullable=True)

def _add_missing_columns(engine):
    """
    Compares the columns in the live 'trips' table with the columns in the
    SQLAlchemy model and adds any that are missing.
    """
    inspector = inspect(engine)
    try:
        # Get the set of column names from the actual database table
        db_columns = {c['name'] for c in inspector.get_columns('trips')}
    except Exception:
        # Table probably doesn't exist yet, create_all will handle it.
        return
    
    # Get the set of column names from the SQLAlchemy model definition
    model_columns = {c.name for c in Trip.__table__.columns}
    
    # Find which columns are defined in the model but not in the database
    missing_columns = model_columns - db_columns

    if missing_columns:
        _LOGGER.info(f"Found missing database columns: {', '.join(missing_columns)}. Updating schema.")
        with engine.connect() as connection:
            for col_name in missing_columns:
                try:
                    # Get the full column object from the model to determine its type
                    model_column = Trip.__table__.columns[col_name]
                    # Compile the SQLAlchemy type into a raw SQL type for the ALTER statement
                    col_type = model_column.type.compile(engine.dialect)
                    _LOGGER.info(f"Adding missing column '{col_name}' with type '{col_type}' to 'trips' table.")
                    # Use a transaction to ensure the operation is atomic
                    with connection.begin():
                        connection.execute(text(f'ALTER TABLE trips ADD COLUMN {col_name} {col_type}'))
                except Exception as e:
                    _LOGGER.error(f"Failed to add column '{col_name}': {e}")
            connection.commit()

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    DATA_DIR.mkdir(exist_ok=True)

    # create_all is safe to call even if the table was created during migration
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

def get_latest_trip_timestamp(vin: str) -> datetime.datetime | None:
    """Gets the start timestamp of the most recent trip for a given VIN."""
    db = SessionLocal()
    try:
        latest_trip = db.query(Trip).filter(
            Trip.vin == vin
        ).order_by(Trip.start_timestamp.desc()).first()
        
        if latest_trip:
            return latest_trip.start_timestamp
        return None
    finally:
        db.close()

def insert_trips(trips: list):
    """
    Inserts a list of trip objects into the database in a single session.
    This is a convenience function for testing and potential bulk operations.
    """
    db = SessionLocal()
    try:
        for trip_data in trips:
            # The test mock might not have all attributes, so we use getattr()
            new_trip = Trip(
                id=trip_data.id,
                vin=trip_data.vin,
                start_timestamp=trip_data.start_timestamp,
                end_timestamp=trip_data.end_timestamp,
                start_address=getattr(trip_data, 'start_address', None),
                end_address=getattr(trip_data, 'end_address', None),
                distance_km=trip_data.distance_km,
                fuel_consumption_l_100km=trip_data.fuel_consumption_l_100km
            )
            db.merge(new_trip)
        db.commit()
        _LOGGER.info(f"Successfully inserted/updated {len(trips)} trips.")
    except Exception as e:
        db.rollback()
        _LOGGER.error(f"Error during bulk trip insert: {e}", exc_info=True)
    finally:
        db.close()

def get_all_trips() -> list:
    """Fetches all trips from the database. Used primarily for testing."""
    db = SessionLocal()
    try:
        # A simple conversion to dict for testing purposes
        return [{"vin": t.vin, "distance_km": t.distance_km} for t in db.query(Trip).all()]
    finally:
        db.close()

def insert_service_history(vin: str, service_histories: list):
    """Inserts or updates service history records for a given VIN."""
    db = SessionLocal()
    try:
        for item in service_histories:
            record = ServiceHistory(
                id=item.get("service_history_id"),
                vin=vin,
                service_date=datetime.datetime.fromisoformat(item.get("service_date")) if item.get("service_date") else None,
                mileage=item.get("mileage"),
                unit=item.get("unit"),
                service_provider=item.get("service_provider"),
                service_category=item.get("service_category"),
                notes=item.get("notes"),
                customer_created_record=item.get("customer_created_record"),
                ro_number=item.get("ro_number"),
                operations_performed=item.get("operations_performed")
            )
            db.merge(record)
        db.commit()
        _LOGGER.info(f"Successfully inserted/updated {len(service_histories)} service history records for VIN {vin}.")
    except Exception as e:
        db.rollback()
        _LOGGER.error(f"Error during service history insert for VIN {vin}: {e}", exc_info=True)
    finally:
        db.close()
