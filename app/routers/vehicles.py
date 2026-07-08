# app/routers/vehicles.py
import json
import logging
import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Body
import aiofiles
import aiofiles.os
from sqlalchemy import func

from .. import fetcher
from .. import database
from .. import time_utils
from ..config import config_manager

_LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/api/vehicles", tags=["vehicles"])

async def get_cached_vehicle_data():
    """Helper to read and return vehicle data from the cache file."""
    if not fetcher.CACHE_FILE.exists():
        _LOGGER.warning("get_cached_vehicle_data: Cache file not found.")
        return []

    try:
        async with aiofiles.open(fetcher.CACHE_FILE, "r") as f:
            content = await f.read()
        data = json.loads(content)
        return data.get("vehicles", [])
    except (json.JSONDecodeError, IOError) as e:
        _LOGGER.error(f"Failed to read or parse cache file: {e}")
        return []

@router.get("")
async def get_vehicle_data():
    """API endpoint to get the cached vehicle data."""
    async with fetcher.CACHE_LOCK:
        if not fetcher.CACHE_FILE.exists():
            return []

        try:
            async with aiofiles.open(fetcher.CACHE_FILE, "r") as f:
                content = await f.read()
                if not content.strip():  # Handle empty file case
                    raise json.JSONDecodeError("Empty file content", "", 0)
                data = json.loads(content)
        except (json.JSONDecodeError, IOError) as e:
            _LOGGER.warning(
                f"Cache file is corrupted or unreadable ({e}). Creating a new one."
            )
            data = {"last_updated": None, "vehicles": []}
            try:
                async with aiofiles.open(fetcher.CACHE_FILE, "w") as f:
                    await f.write(json.dumps(data, indent=2))
            except IOError as io_e:
                _LOGGER.error(f"Could not create new cache file: {io_e}")

        vehicles_data = data.get("vehicles", [])
        last_updated = data.get("last_updated") or "Never"
        
        if last_updated != "Never":
            try:
                dt = datetime.datetime.fromisoformat(last_updated)
                local_dt = time_utils.convert_utc_to_local_naive(dt, config_manager)
                last_updated = local_dt.isoformat()
            except ValueError:
                pass

        # Augment vehicle data with all-time statistics from the database
        db = database.SessionLocal()
        try:
            for vehicle in vehicles_data:
                vehicle["last_updated"] = last_updated
                vin = vehicle.get("vin")
                if not vin:
                    continue

                # Ensure an alias (pretty-name) is always available, defaulting to the VIN
                if not vehicle.get("alias"):
                    vehicle["alias"] = vin

                stats = (
                    db.query(
                        func.sum(database.Trip.distance_km).label("total_distance"),
                        func.sum(database.Trip.ev_distance_km).label(
                            "total_ev_distance"
                        ),
                        func.sum(
                            database.Trip.fuel_consumption_l_100km
                            * database.Trip.distance_km
                            / 100
                        ).label("total_fuel"),
                        func.sum(database.Trip.duration_seconds).label(
                            "total_duration_seconds"
                        ),
                        func.max(database.Trip.max_speed_kmh).label(
                            "overall_max_speed"
                        ),
                        func.sum(database.Trip.length_highway_km).label(
                            "total_highway_distance"
                        ),
                    )
                    .filter(database.Trip.vin == vin)
                    .first()
                )

                _LOGGER.debug(f"--- Overall Stats for VIN: {vin} ---")
                _LOGGER.debug(f"Raw DB stats: {stats}")

                # Fetch and process countries separately, ensuring we only query valid JSON.
                countries_results = (
                    db.query(database.Trip.countries)
                    .filter(
                        database.Trip.vin == vin,
                        database.Trip.countries.is_not(None),
                        database.Trip.countries != "",
                    )
                    .all()
                )
                all_countries = set()
                for res in countries_results:
                    if res[0]:
                        all_countries.update(res[0])
                sorted_countries = sorted(list(all_countries))

                vehicle["statistics"]["overall"] = {}
                if stats and stats.total_distance is not None:
                    total_distance = stats.total_distance
                    total_ev_distance = stats.total_ev_distance or 0.0
                    total_fuel = stats.total_fuel or 0.0
                    total_duration_seconds = stats.total_duration_seconds or 0
                    total_highway_distance = stats.total_highway_distance or 0.0

                    vehicle["statistics"]["overall"]["total_ev_distance_km"] = round(
                        total_ev_distance
                    )
                    vehicle["statistics"]["overall"]["total_fuel_l"] = round(
                        total_fuel, 2
                    )
                    vehicle["statistics"]["overall"]["total_duration_seconds"] = (
                        total_duration_seconds
                    )
                    vehicle["statistics"]["overall"]["total_highway_distance_km"] = (
                        round(total_highway_distance)
                    )
                    if stats.overall_max_speed is not None:
                        vehicle["statistics"]["overall"]["overall_max_speed_kmh"] = (
                            round(stats.overall_max_speed)
                        )
                    vehicle["statistics"]["overall"]["countries"] = (
                        ", ".join(sorted_countries) if sorted_countries else "N/A"
                    )

                    if total_distance > 0:
                        vehicle["statistics"]["overall"]["ev_ratio_percent"] = round(
                            (total_ev_distance / total_distance) * 100, 1
                        )
                        vehicle["statistics"]["overall"]["highway_ratio_percent"] = (
                            round((total_highway_distance / total_distance) * 100, 1)
                        )
                    else:
                        vehicle["statistics"]["overall"]["highway_ratio_percent"] = 0

                    if total_distance > 0 and total_fuel > 0:
                        vehicle["statistics"]["overall"]["fuel_consumption_l_100km"] = (
                            round((total_fuel / total_distance) * 100, 2)
                        )
                    _LOGGER.debug(
                        f"Final overall stats object: {vehicle['statistics']['overall']}"
                    )
                else:
                    _LOGGER.debug(
                        "No trip data found for this VIN, skipping overall stats calculation."
                    )
        finally:
            db.close()

        return vehicles_data

@router.get("/{vin}/history")
def get_vehicle_history(vin: str, days: int = 30):
    """API endpoint to get historical data for a vehicle."""
    db = database.SessionLocal()
    try:
        start_date = time_utils.get_naive_utc_now(config_manager) - datetime.timedelta(days=days)
        filters = [database.VehicleReading.timestamp >= start_date]
        if vin != "all":
            filters.append(database.VehicleReading.vin == vin)
            
        readings = (
            db.query(database.VehicleReading)
            .filter(*filters)
            .order_by(database.VehicleReading.timestamp.asc())
            .all()
        )
        
        # Detach the models from the session before modifying them for presentation
        db.expunge_all()
        
        for reading in readings:
            reading.timestamp = time_utils.convert_utc_to_local_naive(reading.timestamp, config_manager)
        return readings
    finally:
        db.close()

@router.get("/{vin}/daily_summary")
def get_daily_summary(vin: str, period: str = "30"):
    """
    API endpoint to get a summary of distance and fuel consumption per day.
    The date range is automatically clipped to the available data.
    """
    db = database.SessionLocal()
    try:
        days: Optional[int] = None
        if period.isdigit():
            days = int(period)
        elif period != "all":
            raise HTTPException(status_code=400, detail="Invalid period specified.")

        # First, find the absolute earliest trip for this VIN to use as a boundary.
        if vin == "all":
            earliest_trip_ts = db.query(func.min(database.Trip.start_timestamp)).scalar()
        else:
            earliest_trip_ts = (
                db.query(func.min(database.Trip.start_timestamp))
                .filter(database.Trip.vin == vin)
                .scalar()
            )

        if not earliest_trip_ts:
            _LOGGER.info(
                f"No trip data found for VIN {vin}. Returning empty daily summary."
            )
            return []

        # Determine the start date for the query filter.
        actual_start_date_filter = earliest_trip_ts
        if days is not None:
            # If a specific period is requested, find the later of the two dates.
            requested_start_date = time_utils.get_naive_utc_now(config_manager) - datetime.timedelta(
                days=days
            )
            actual_start_date_filter = max(earliest_trip_ts, requested_start_date)

        # Build the main query for trips within the determined date range.
        filters = [database.Trip.start_timestamp >= actual_start_date_filter]
        if vin != "all":
            filters.append(database.Trip.vin == vin)
            
        offset_str = time_utils.get_sqlite_offset_string(config_manager)
        local_timestamp = func.datetime(database.Trip.start_timestamp, offset_str)
            
        trips_query = (
            db.query(
                func.date(local_timestamp).label("day"),
                func.sum(database.Trip.distance_km).label("distance"),
                func.sum(
                    database.Trip.fuel_consumption_l_100km
                    * database.Trip.distance_km
                    / 100
                ).label("fuel"),
                func.sum(database.Trip.ev_distance_km).label("ev_distance"),
                func.sum(database.Trip.ev_duration_seconds).label("ev_duration"),
                func.avg(database.Trip.score_global).label("avg_score"),
                func.sum(database.Trip.duration_seconds).label("total_duration"),
                func.max(database.Trip.max_speed_kmh).label("max_speed"),
            )
            .filter(*filters)
            .group_by(func.date(local_timestamp))
            .all()
        )

        # Create a dictionary with default zero values for every day in the date range.
        daily_data = {}
        start_date_for_range = actual_start_date_filter.date()
        end_date_for_range = time_utils.get_naive_utc_now(config_manager).date()
        num_days_in_range = (end_date_for_range - start_date_for_range).days + 1

        if num_days_in_range > 0:
            for i in range(num_days_in_range):
                current_date = start_date_for_range + datetime.timedelta(days=i)
                daily_data[current_date.isoformat()] = {
                    "distance": 0.0,
                    "fuel": 0.0,
                    "ev_distance": 0.0,
                    "ev_duration": 0,
                    "score": None,
                    "duration_seconds": 0,
                    "max_speed": None,
                }

        # Update the dictionary with actual data from the query.
        for r in trips_query:
            day_str = r.day
            if day_str in daily_data:
                daily_data[day_str]["distance"] = r.distance or 0.0
                daily_data[day_str]["fuel"] = r.fuel or 0.0
                daily_data[day_str]["ev_distance"] = r.ev_distance or 0.0
                daily_data[day_str]["ev_duration"] = r.ev_duration or 0
                daily_data[day_str]["score"] = r.avg_score
                daily_data[day_str]["duration_seconds"] = r.total_duration or 0
                daily_data[day_str]["max_speed"] = r.max_speed

        # Format the final list for the frontend.
        return [
            {
                "date": day,
                "distance_km": round(data["distance"], 2),
                "fuel_total_l": round(data["fuel"], 2),
                "fuel_consumption_l_100km": round(
                    (data["fuel"] / data["distance"]) * 100, 2
                )
                if data["fuel"] > 0 and data["distance"] > 0
                else 0.0,
                "ev_distance_km": round(data.get("ev_distance", 0), 2),
                "ev_duration_seconds": data.get("ev_duration", 0),
                "score_global": round(data["score"], 0)
                if data.get("score") is not None
                else None,
                "duration_seconds": data.get("duration_seconds", 0),
                "average_speed_kmh": round(
                    data["distance"] / (data["duration_seconds"] / 3600), 2
                )
                if data.get("duration_seconds", 0) > 0 and data["distance"] > 0
                else 0.0,
                "max_speed_kmh": data.get("max_speed"),
            }
            for day, data in sorted(daily_data.items())
        ]
    finally:
        db.close()

@router.get("/{vin}/trip_count")
def get_trip_count(vin: str, period: str = "30"):
    """
    API endpoint to get the total count of individual trips for a given period.
    """
    db = database.SessionLocal()
    try:
        days: Optional[int] = None
        if period.isdigit():
            days = int(period)
        elif period != "all":
            return {"trip_count": 0}  # Should not happen with current UI

        # Find the absolute earliest trip for this VIN to use as a boundary.
        if vin == "all":
            earliest_trip_ts = db.query(func.min(database.Trip.start_timestamp)).scalar()
        else:
            earliest_trip_ts = (
                db.query(func.min(database.Trip.start_timestamp))
                .filter(database.Trip.vin == vin)
                .scalar()
            )

        if not earliest_trip_ts:
            return {"trip_count": 0}

        # Determine the start date for the query filter.
        start_date_filter = earliest_trip_ts
        if days is not None:
            requested_start_date = time_utils.get_naive_utc_now(config_manager) - datetime.timedelta(
                days=days
            )
            start_date_filter = max(earliest_trip_ts, requested_start_date)

        # Perform the count query
        filters = [database.Trip.start_timestamp >= start_date_filter]
        if vin != "all":
            filters.append(database.Trip.vin == vin)
            
        count = (
            db.query(database.Trip)
            .filter(*filters)
            .count()
        )

        return {"trip_count": count}
    finally:
        db.close()

@router.get("/{vin}/countries")
def get_available_countries(vin: str):
    """Gets a unique, sorted list of country codes for all trips for a given VIN."""
    db = database.SessionLocal()
    try:
        filters = [database.Trip.countries.is_not(None), database.Trip.countries != ""]
        if vin != "all":
            filters.append(database.Trip.vin == vin)
            
        results = (
            db.query(database.Trip.countries)
            .filter(*filters)
            .distinct()
            .all()
        )

        unique_countries = set()
        for res in results:
            if res[0]:
                unique_countries.update(res[0])

        return sorted(list(unique_countries))
    finally:
        db.close()

@router.get("/{vin}/trip_data")
def get_trip_data(
    vin: str, period: str = "30", metric: str = "fuel_consumption_l_100km"
):
    """
    API endpoint to get a raw list of a single metric's values from all individual trips in a period.
    """
    # Validate the requested metric against the Trip model to ensure it's a safe, valid column.
    valid_metrics = [c.name for c in database.Trip.__table__.columns]
    if metric not in valid_metrics:
        raise HTTPException(
            status_code=400, detail=f"Invalid metric specified: {metric}"
        )

    db = database.SessionLocal()
    try:
        days: Optional[int] = None
        if period.isdigit():
            days = int(period)
        elif period != "all":
            return {"values": []}

        if vin == "all":
            earliest_trip_ts = db.query(func.min(database.Trip.start_timestamp)).scalar()
        else:
            earliest_trip_ts = (
                db.query(func.min(database.Trip.start_timestamp))
                .filter(database.Trip.vin == vin)
                .scalar()
            )
            
        if not earliest_trip_ts:
            return {"values": []}

        start_date_filter = earliest_trip_ts
        if days is not None:
            requested_start_date = time_utils.get_naive_utc_now(config_manager) - datetime.timedelta(
                days=days
            )
            start_date_filter = max(earliest_trip_ts, requested_start_date)

        # Query for the single column of data.
        filters = [database.Trip.start_timestamp >= start_date_filter]
        if vin != "all":
            filters.append(database.Trip.vin == vin)
            
        query_result = (
            db.query(getattr(database.Trip, metric))
            .filter(*filters)
            .all()
        )

        # The result is a list of tuples, e.g., [(5.5,), (6.1,)]. This flattens it to [5.5, 6.1].
        values = [item[0] for item in query_result if item[0] is not None]

        return {"values": values}
    finally:
        db.close()

@router.get("/{vin}/heatmap")
def get_heatmap_data(vin: str):
    """
    Fetches all GPS route points for a vehicle to generate a heatmap.
    """
    db = database.SessionLocal()
    try:
        # Query for all trips for the given VIN that have route data
        filters = [database.Trip.route.is_not(None)]
        if vin != "all":
            filters.append(database.Trip.vin == vin)
            
        trips_with_routes = (
            db.query(database.Trip.route)
            .filter(*filters)
            .all()
        )

        all_points = []
        for trip_route in trips_with_routes:
            # The route is stored as a list of points in the first element of the tuple
            route_points = trip_route[0]
            if isinstance(route_points, list):
                for point in route_points:
                    # Add each point as a [lat, lon] list
                    if isinstance(point, dict) and "lat" in point and "lon" in point:
                        all_points.append([point["lat"], point["lon"]])

        _LOGGER.info(f"Returning {len(all_points)} points for VIN {vin} heatmap.")
        return all_points
    finally:
        db.close()

@router.post("/{vin}/fetch_trips")
async def trigger_trip_fetch(vin: str, period_data: dict = Body(...)):
    """Triggers a manual, on-demand fetch of historical trip data."""
    period = period_data.get("period")
    if not period:
        raise HTTPException(status_code=400, detail="Missing 'period' in request body.")

    try:
        result = await fetcher.backfill_trips(vin=vin, period=period)
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except Exception as e:
        logging.error(
            f"Error during manual trip backfill for VIN {vin}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail="An internal error occurred during the trip fetch."
        )

@router.post("/{vin}/service_history")
async def trigger_service_history_fetch(vin: str):
    """Fetches service history and updates the vehicle cache."""
    history_data = await fetcher.fetch_service_history(vin=vin)
    if "error" in history_data:
        raise HTTPException(status_code=500, detail=history_data["error"])

    async with fetcher.CACHE_LOCK:
        try:
            async with aiofiles.open(fetcher.CACHE_FILE, "r") as f:
                content = await f.read()
                data = json.loads(content)
        except (IOError, json.JSONDecodeError):
            _LOGGER.warning(
                "Could not open cache file to save service history, returning live data only."
            )
            return history_data

        vehicle_found = False
        for vehicle in data.get("vehicles", []):
            if vehicle.get("vin") == vin:
                vehicle["service_history"] = history_data.get("service_histories", [])
                vehicle_found = True
                break

        if not vehicle_found:
            _LOGGER.warning(
                f"VIN {vin} not found in cache file. Unable to save service history."
            )
            return history_data

        try:
            CACHE_FILE_TMP = fetcher.CACHE_FILE.with_suffix(".tmp")
            async with aiofiles.open(CACHE_FILE_TMP, "w") as f:
                await f.write(json.dumps(data, indent=2))
            await aiofiles.os.replace(CACHE_FILE_TMP, fetcher.CACHE_FILE)
            _LOGGER.info(
                f"Successfully fetched and saved service history for VIN {vin}."
            )
        except IOError as e:
            _LOGGER.error(
                f"Failed to write updated cache file with service history: {e}"
            )

    return history_data
