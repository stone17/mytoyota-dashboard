# app/routers/trips.py
import datetime
import csv
import io
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_
from sqlalchemy.orm import defer

from .. import database
from .. import time_utils
from ..config import config_manager

router = APIRouter(prefix="/api", tags=["trips"])

@router.get("/trips")
def get_trips(
    vin: str,
    sort_by: str = "start_timestamp",
    sort_direction: str = "desc",
    unit_system: str = "metric",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    countries: Optional[str] = Query(None),
):
    """API endpoint to get all imported trips for a vehicle, with date and country filtering."""
    db = database.SessionLocal()
    try:
        from sqlalchemy import text

        valid_sort_columns = {c.name for c in database.Trip.__table__.columns}
        if sort_by not in valid_sort_columns:
            raise HTTPException(status_code=400, detail=f"Invalid sort_by parameter.")

        sort_column_name = {
            "distance_km": "distance_mi"
            if unit_system.startswith("imperial")
            else "distance_km",
            "fuel_consumption_l_100km": "mpg_uk"
            if unit_system == "imperial_uk"
            else (
                "mpg" if unit_system == "imperial_us" else "fuel_consumption_l_100km"
            ),
            "average_speed_kmh": "average_speed_mph"
            if unit_system.startswith("imperial")
            else "average_speed_kmh",
            "ev_distance_km": "ev_distance_mi"
            if unit_system.startswith("imperial")
            else "ev_distance_km",
        }.get(sort_by, sort_by)

        sort_expression = None
        if sort_by == "fuel_consumption_l_100km":
            if unit_system.startswith("imperial"):
                if sort_direction == "desc":
                    sort_expression = text(
                        f"CASE WHEN {sort_column_name} IS NULL OR {sort_column_name} = 0 THEN 1 ELSE 0 END, {sort_column_name} DESC"
                    )
                else:
                    sort_expression = text(
                        f"CASE WHEN {sort_column_name} IS NULL OR {sort_column_name} = 0 THEN 0 ELSE 1 END, {sort_column_name} ASC"
                    )
            else:
                if sort_direction == "desc":
                    sort_expression = text(
                        f"CASE WHEN {sort_column_name} IS NULL THEN 1 ELSE 0 END, {sort_column_name} ASC"
                    )
                else:
                    sort_expression = text(
                        f"CASE WHEN {sort_column_name} IS NULL THEN 1 ELSE 0 END, {sort_column_name} DESC"
                    )
        else:
            direction_sql = "DESC" if sort_direction == "desc" else "ASC"
            sort_expression = text(f"{sort_column_name} {direction_sql} NULLS LAST")

        # Base query
        query = db.query(database.Trip).options(defer(database.Trip.route))
        if vin != "all":
            query = query.filter(database.Trip.vin == vin)

        # Apply date filters if provided
        if start_date:
            try:
                start_dt = datetime.datetime.fromisoformat(start_date).replace(
                    hour=0, minute=0, second=0
                )
                query = query.filter(database.Trip.start_timestamp >= start_dt)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD."
                )
        if end_date:
            try:
                end_dt = datetime.datetime.fromisoformat(end_date).replace(
                    hour=23, minute=59, second=59
                )
                query = query.filter(database.Trip.start_timestamp <= end_dt)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD."
                )

        # Apply country filter if provided
        if countries:
            country_list = [c.strip() for c in countries.split(",") if c.strip()]
            if country_list:
                country_filters = [
                    func.instr(database.Trip.countries, f'"{country}"') > 0
                    for country in country_list
                ]
                query = query.filter(or_(*country_filters))

        # Apply sorting and fetch all results
        trips = query.order_by(sort_expression).all()

        # Convert timestamps from naive UTC to naive local timezone for frontend display
        for trip in trips:
            trip.start_timestamp = time_utils.convert_utc_to_local_naive(trip.start_timestamp, config_manager)
            trip.end_timestamp = time_utils.convert_utc_to_local_naive(trip.end_timestamp, config_manager)

        # This prevents "N/A" on the frontend if the backfill hasn't run for new trips.
        if unit_system.startswith("imperial"):
            KM_TO_MI = 0.621371
            for trip in trips:
                if trip.distance_km is not None:
                    trip.distance_mi = trip.distance_km * KM_TO_MI
                if trip.ev_distance_km is not None:
                    trip.ev_distance_mi = trip.ev_distance_km * KM_TO_MI
                if trip.average_speed_kmh is not None:
                    trip.average_speed_mph = trip.average_speed_kmh * KM_TO_MI

                # Check for fuel consumption to avoid division by zero
                if trip.fuel_consumption_l_100km and trip.fuel_consumption_l_100km > 0:
                    trip.mpg = 235.214 / trip.fuel_consumption_l_100km
                    trip.mpg_uk = 282.481 / trip.fuel_consumption_l_100km
                else:
                    # Assign a default value if no fuel was consumed
                    trip.mpg = 0.0
                    trip.mpg_uk = 0.0

        return trips
    finally:
        db.close()

@router.get("/trips/{trip_id}/route")
def get_trip_route(trip_id: int):
    """Fetches the route data for a single trip."""
    db = database.SessionLocal()
    try:
        # Query only the 'route' column for efficiency
        trip = db.query(database.Trip.route).filter(database.Trip.id == trip_id).first()
        if not trip:
            raise HTTPException(status_code=404, detail="Trip not found")
        return {"route": trip.route}
    finally:
        db.close()

@router.post("/import/trips")
async def import_trips_from_csv(file: UploadFile = File(...)):
    """
    Imports historical trip data from a CSV file exported from the Toyota app.
    The filename is expected to contain the VIN (e.g., 'VIN_YYYY-MM-DD_YYYY-MM-DD.csv').
    """
    filename = file.filename
    try:
        vin = filename.split("_")[0]
        if (
            not (vin.startswith("SB") or vin.startswith("JT")) or len(vin) < 17
        ):  # Basic VIN check
            raise ValueError("Filename does not appear to contain a valid VIN.")
    except (IndexError, ValueError) as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid filename format. Expected 'VIN_start-date_end-date.csv'. Error: {e}",
        )

    content = await file.read()
    content_text = content.decode("utf-8")
    file_like_object = io.StringIO(content_text)
    reader = csv.reader(file_like_object, delimiter=";")

    db = database.SessionLocal()
    imported_count = 0
    updated_count = 0
    skipped_count = 0

    try:
        next(reader)  # Skip header
        for row in reader:
            try:
                if len(row) < 6:
                    skipped_count += 1
                    continue

                # Parse all data from the CSV row first
                start_address_csv = row[0]
                end_address_csv = row[2]
                distance_csv = float(row[4].replace(",", "."))
                start_ts_utc = time_utils.convert_to_local_naive(datetime.datetime.fromisoformat(row[1]), config_manager)
                end_ts_utc = time_utils.convert_to_local_naive(datetime.datetime.fromisoformat(row[3]), config_manager)
                fuel_consumption_csv = float(row[5].replace(",", "."))

                # --- Content-Based Deduplication Logic ---
                # Find a trip with the same addresses and a very similar distance.
                distance_tolerance = 0.1  # 100 meters tolerance for small variations

                existing_trip = (
                    db.query(database.Trip)
                    .filter(
                        database.Trip.vin == vin,
                        database.Trip.start_address == start_address_csv,
                        database.Trip.end_address == end_address_csv,
                        database.Trip.distance_km.between(
                            distance_csv - distance_tolerance,
                            distance_csv + distance_tolerance,
                        ),
                    )
                    .first()
                )

                if existing_trip:
                    # This is a duplicate trip, so we skip it.
                    skipped_count += 1
                else:
                    # This is a unique trip, so we insert it.
                    new_trip = database.Trip(
                        vin=vin,
                        start_timestamp=start_ts_utc,
                        end_timestamp=end_ts_utc,
                        start_address=start_address_csv,
                        end_address=end_address_csv,
                        distance_km=distance_csv,
                        fuel_consumption_l_100km=fuel_consumption_csv,
                    )
                    db.add(new_trip)
                    imported_count += 1
            except (ValueError, IndexError):
                skipped_count += 1

        db.commit()  # Commit the entire transaction once at the end.
        return {
            "message": "Import complete.",
            "imported": imported_count,
            "updated": updated_count,
            "skipped_duplicates_or_errors": skipped_count,
        }
    except Exception as e:
        db.rollback()
        logging.error(f"Error during CSV import transaction: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="A critical error occurred during import. The entire operation was rolled back.",
        )
    finally:
        db.close()

@router.get("/export/trips.csv")
def export_trips_to_csv(
    vin: str,
    unit_system: str = "metric",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    countries: Optional[str] = Query(None),
):
    """
    Exports the currently filtered list of trips to a CSV file.
    This endpoint re-uses the same filtering logic as the get_trips endpoint.
    """
    # Use a temporary database session
    db = database.SessionLocal()
    try:
        # --- 1. Fetch filtered trip data (logic copied from get_trips) ---
        query = db.query(database.Trip).options(defer(database.Trip.route))
        if vin != "all":
            query = query.filter(database.Trip.vin == vin)

        if start_date:
            start_dt = datetime.datetime.fromisoformat(start_date).replace(
                hour=0, minute=0, second=0
            )
            query = query.filter(database.Trip.start_timestamp >= start_dt)
        if end_date:
            end_dt = datetime.datetime.fromisoformat(end_date).replace(
                hour=23, minute=59, second=59
            )
            query = query.filter(database.Trip.start_timestamp <= end_dt)

        if countries:
            country_list = [c.strip() for c in countries.split(",") if c.strip()]
            if country_list:
                country_filters = [
                    func.instr(database.Trip.countries, f'"{country}"') > 0
                    for country in country_list
                ]
                query = query.filter(or_(*country_filters))

        trips = query.order_by(database.Trip.start_timestamp.desc()).all()

        # --- 2. Prepare CSV data in memory ---
        output = io.StringIO()
        writer = csv.writer(output, delimiter=";")

        is_imperial = unit_system.startswith("imperial")
        is_uk = unit_system == "imperial_uk"
        dist_unit = "mi" if is_imperial else "km"
        speed_unit = "mph" if is_imperial else "kmh"
        consumption_unit = (
            f"mpg_{'uk' if is_uk else 'us'}" if is_imperial else "l_100km"
        )

        # --- 3. Write CSV Header ---
        headers = [
            "start_timestamp_utc",
            "end_timestamp_utc",
            "start_address",
            "end_address",
            f"distance_{dist_unit}",
            f"consumption_{consumption_unit}",
            "duration_seconds",
            f"average_speed_{speed_unit}",
            f"max_speed_{speed_unit}",
            f"ev_distance_{dist_unit}",
            "ev_duration_seconds",
            "score_global",
            "score_acceleration",
            "score_braking",
            "score_constant_speed",
            "night_trip",
            "countries",
            f"overspeed_distance_{dist_unit}",
            "overspeed_duration_seconds",
            f"highway_distance_{dist_unit}",
            "highway_duration_seconds",
            f"hdc_eco_distance_{dist_unit}",
            "hdc_eco_duration_seconds",
            f"hdc_power_distance_{dist_unit}",
            "hdc_power_duration_seconds",
            f"hdc_charge_distance_{dist_unit}",
            "hdc_charge_duration_seconds",
        ]
        writer.writerow(headers)

        # --- 4. Write Data Rows ---
        for trip in trips:
            # Perform unit conversions on the fly for the export
            if is_imperial:
                trip.distance_mi = (
                    trip.distance_km * 0.621371
                    if trip.distance_km is not None
                    else None
                )
                trip.average_speed_mph = (
                    trip.average_speed_kmh * 0.621371
                    if trip.average_speed_kmh is not None
                    else None
                )
                trip.max_speed_mph = (
                    trip.max_speed_kmh * 0.621371
                    if trip.max_speed_kmh is not None
                    else None
                )
                trip.ev_distance_mi = (
                    trip.ev_distance_km * 0.621371
                    if trip.ev_distance_km is not None
                    else None
                )
                trip.length_overspeed_mi = (
                    trip.length_overspeed_km * 0.621371
                    if trip.length_overspeed_km is not None
                    else None
                )
                trip.length_highway_mi = (
                    trip.length_highway_km * 0.621371
                    if trip.length_highway_km is not None
                    else None
                )
                trip.hdc_eco_distance_mi = (
                    trip.hdc_eco_distance_km * 0.621371
                    if trip.hdc_eco_distance_km is not None
                    else None
                )
                trip.hdc_power_distance_mi = (
                    trip.hdc_power_distance_km * 0.621371
                    if trip.hdc_power_distance_km is not None
                    else None
                )
                trip.hdc_charge_distance_mi = (
                    trip.hdc_charge_distance_km * 0.621371
                    if trip.hdc_charge_distance_km is not None
                    else None
                )

                if trip.fuel_consumption_l_100km and trip.fuel_consumption_l_100km > 0:
                    trip.mpg_us = 235.214 / trip.fuel_consumption_l_100km
                    trip.mpg_uk = 282.481 / trip.fuel_consumption_l_100km
                else:
                    trip.mpg_us = 0.0
                    trip.mpg_uk = 0.0

            row = [
                trip.start_timestamp,
                trip.end_timestamp,
                trip.start_address,
                trip.end_address,
                trip.distance_mi if is_imperial else trip.distance_km,
                trip.mpg_uk
                if is_uk
                else (trip.mpg_us if is_imperial else trip.fuel_consumption_l_100km),
                trip.duration_seconds,
                trip.average_speed_mph if is_imperial else trip.average_speed_kmh,
                trip.max_speed_mph if is_imperial else trip.max_speed_kmh,
                trip.ev_distance_mi if is_imperial else trip.ev_distance_km,
                trip.ev_duration_seconds,
                trip.score_global,
                trip.score_acceleration,
                trip.score_braking,
                trip.score_constant_speed,
                trip.night_trip,
                ",".join(trip.countries) if trip.countries else "",
                trip.length_overspeed_mi if is_imperial else trip.length_overspeed_km,
                trip.duration_overspeed_seconds,
                trip.length_highway_mi if is_imperial else trip.length_highway_km,
                trip.duration_highway_seconds,
                trip.hdc_eco_distance_mi if is_imperial else trip.hdc_eco_distance_km,
                trip.hdc_eco_duration_seconds,
                trip.hdc_power_distance_mi
                if is_imperial
                else trip.hdc_power_distance_km,
                trip.hdc_power_duration_seconds,
                trip.hdc_charge_distance_mi
                if is_imperial
                else trip.hdc_charge_distance_km,
                trip.hdc_charge_duration_seconds,
            ]
            writer.writerow(row)

        output.seek(0)
        # --- 5. Return the CSV file as a streaming response ---
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=trips_export_{vin}_{datetime.date.today()}.csv"
            },
        )
    finally:
        db.close()
