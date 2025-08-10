# app/main.py
import asyncio
import json
import csv
import io
import yaml
import time
import datetime
import logging
from collections import deque
from typing import Deque, Dict, Optional

import aiofiles
from fastapi import FastAPI, HTTPException, Request, Body, UploadFile, File, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_

from . import fetcher
from . import database
from . import mqtt
from .credentials_manager import get_username, save_credentials
from .config import settings, load_config, USER_CONFIG_PATH
from .logging_config import setup_logging
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import defer

# Configure logging at the very beginning of the application startup
setup_logging()
_LOGGER = logging.getLogger(__name__)

# --- Live Log Streaming Setup ---
# Get the desired log history size from config, with a sensible default.
log_history_size = settings.get("log_history_size", 200)
# A thread-safe, memory-efficient deque to hold the last N log messages for new clients.
log_history: Deque[Dict] = deque(maxlen=log_history_size)
# An asyncio queue for broadcasting new log messages to connected clients.
log_queue = asyncio.Queue()

class WebLogHandler(logging.Handler):
    """A custom logging handler that captures logs for the web UI."""
    def emit(self, record):
        """Formats the log record and puts it into our history and live queue."""
        log_entry = {
            "level": record.levelname,
            "message": self.format(record)
        }
        log_history.append(log_entry)
        try:
            # Use put_nowait to avoid blocking in the synchronous logging call.
            log_queue.put_nowait(log_entry)
        except asyncio.QueueFull:
            # This is unlikely to happen with a default queue size but is a safe fallback.
            pass

# Get the root logger and add our custom handler to capture all logs.
web_log_handler = WebLogHandler()
web_log_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(web_log_handler)

app = FastAPI()

# Mount static files (CSS, JS)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Setup templates
templates = Jinja2Templates(directory="app/templates")

async def schedule_fetch():
    """Runs the data fetcher on a schedule."""
    while True:
        try:
            # MODIFIED: Call the new unified fetch cycle function.
            await fetcher.run_fetch_cycle()
        except Exception as e:
            logging.error(f"Error in scheduled fetch: {e}", exc_info=True)

        web_server_settings = settings.get("web_server", {})
        polling_settings = web_server_settings.get("polling", {})
        mode = polling_settings.get("mode", "interval")

        if mode == "fixed_time":
            now = datetime.datetime.now()
            target_time_str = polling_settings.get("fixed_time", "07:00")
            hour, minute = map(int, target_time_str.split(':'))

            target_today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if now >= target_today:
                target_next = target_today + datetime.timedelta(days=1)
            else:
                target_next = target_today

            sleep_duration = (target_next - now).total_seconds()
            logging.info(f"Next poll scheduled for {target_next}. Sleeping for {int(sleep_duration)} seconds.")
            await asyncio.sleep(sleep_duration)
        else: # Default to interval mode
            # Fallback to the old key for backward compatibility
            interval = polling_settings.get("interval_seconds") or web_server_settings.get("data_refresh_interval_seconds", 3600)
            logging.info(f"Next poll in {interval} seconds.")
            await asyncio.sleep(interval)

@app.on_event("startup")
async def startup_event():
    """On startup, run an immediate fetch and then schedule periodic updates."""
    logging.info("Initializing database...")
    database.init_db()
    logging.info("Application startup...")

    web_server_settings = settings.get("web_server", {})
    polling_settings = web_server_settings.get("polling", {})
    # Fallback to the old key for backward compatibility
    refresh_interval = polling_settings.get("interval_seconds") or web_server_settings.get("data_refresh_interval_seconds", 3600)
    time_since_last_fetch = float('inf')

    if fetcher.CACHE_FILE.exists():
        last_modified_time = fetcher.CACHE_FILE.stat().st_mtime
        time_since_last_fetch = time.time() - last_modified_time

    if time_since_last_fetch >= refresh_interval:
        logging.info("Cache is stale or missing. Triggering immediate data fetch.")
        asyncio.create_task(schedule_fetch())
    else:
        wait_time = refresh_interval - time_since_last_fetch
        logging.info(f"Cache is fresh. Scheduling first fetch in {int(wait_time)} seconds.")
        
        async def delayed_schedule_fetch():
            await asyncio.sleep(wait_time)
            await schedule_fetch()

        asyncio.create_task(delayed_schedule_fetch())

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the main HTML page."""
    # Add a cache-busting query parameter for development
    cache_buster = int(time.time())
    return templates.TemplateResponse("index.html", {"request": request, "cache_buster": cache_buster})

@app.get("/settings", response_class=HTMLResponse)
async def read_settings(request: Request):
    """Serve the settings page."""
    # Add a cache-busting query parameter for development
    cache_buster = int(time.time())
    return templates.TemplateResponse("settings.html", {"request": request, "cache_buster": cache_buster})

@app.get("/trips", response_class=HTMLResponse)
async def read_trips(request: Request):
    """Serve the trip history page."""
    # Add a cache-busting query parameter for development
    cache_buster = int(time.time())
    return templates.TemplateResponse("trips.html", {"request": request, "cache_buster": cache_buster})

@app.get("/logs", response_class=HTMLResponse)
async def read_logs_page(request: Request):
    """Serve the logs page."""
    cache_buster = int(time.time())
    return templates.TemplateResponse("logs.html", {"request": request, "cache_buster": cache_buster})

@app.get("/notifications", response_class=HTMLResponse)
async def read_notifications_page(request: Request):
    """Serve the notifications page."""
    cache_buster = int(time.time())
    return templates.TemplateResponse("notifications.html", {"request": request, "cache_buster": cache_buster})

@app.get("/heatmap", response_class=HTMLResponse)
async def read_heatmap_page(request: Request):
    """Serve the heatmap page."""
    cache_buster = int(time.time())
    return templates.TemplateResponse("heatmap.html", {"request": request, "cache_buster": cache_buster})

async def get_cached_vehicle_data():
    """Helper to read and return vehicle data from the cache file."""
    if not fetcher.CACHE_FILE.exists():
        _LOGGER.warning("get_cached_vehicle_data: Cache file not found.")
        return []

    try:
        async with aiofiles.open(fetcher.CACHE_FILE, 'r') as f:
            content = await f.read()
        data = json.loads(content)
        return data.get("vehicles", [])
    except (json.JSONDecodeError, IOError) as e:
        _LOGGER.error(f"Failed to read or parse cache file: {e}")
        return []

@app.get("/api/vehicles")
async def get_vehicle_data():
    """API endpoint to get the cached vehicle data."""
    async with fetcher.CACHE_LOCK:
        if not fetcher.CACHE_FILE.exists():
            return []
        
        try:
            async with aiofiles.open(fetcher.CACHE_FILE, 'r') as f:
                content = await f.read()
                if not content.strip(): # Handle empty file case
                    raise json.JSONDecodeError("Empty file content", "", 0)
                data = json.loads(content)
        except (json.JSONDecodeError, IOError) as e:
            _LOGGER.warning(f"Cache file is corrupted or unreadable ({e}). Creating a new one.")
            data = {"last_updated": None, "vehicles": []}
            try:
                async with aiofiles.open(fetcher.CACHE_FILE, 'w') as f:
                    await f.write(json.dumps(data, indent=2))
            except IOError as io_e:
                _LOGGER.error(f"Could not create new cache file: {io_e}")
        
        vehicles_data = data.get("vehicles", [])
        last_updated = data.get("last_updated") or "Never"

        # Augment vehicle data with all-time statistics from the database
        db = database.SessionLocal()
        try:
            from sqlalchemy import func
            for vehicle in vehicles_data:
                vehicle["last_updated"] = last_updated
                vin = vehicle.get("vin")
                if not vin:
                    continue
                
                stats = db.query(
                    func.sum(database.Trip.distance_km).label("total_distance"),
                    func.sum(database.Trip.ev_distance_km).label("total_ev_distance"),
                    func.sum(database.Trip.fuel_consumption_l_100km * database.Trip.distance_km / 100).label("total_fuel"),
                    func.sum(database.Trip.duration_seconds).label("total_duration_seconds"),
                    func.max(database.Trip.max_speed_kmh).label("overall_max_speed"),
                    func.sum(database.Trip.length_highway_km).label("total_highway_distance")
                ).filter(database.Trip.vin == vin).first()
                
                _LOGGER.debug(f"--- Overall Stats for VIN: {vin} ---")
                _LOGGER.debug(f"Raw DB stats: {stats}")

                # Fetch and process countries separately, ensuring we only query valid JSON.
                countries_results = db.query(database.Trip.countries).filter(
                    database.Trip.vin == vin,
                    database.Trip.countries.is_not(None),
                    database.Trip.countries != ''
                ).all()
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
                    
                    vehicle["statistics"]["overall"]["total_ev_distance_km"] = round(total_ev_distance)
                    vehicle["statistics"]["overall"]["total_fuel_l"] = round(total_fuel, 2)
                    vehicle["statistics"]["overall"]["total_duration_seconds"] = total_duration_seconds
                    vehicle["statistics"]["overall"]["total_highway_distance_km"] = round(total_highway_distance)
                    if stats.overall_max_speed is not None:
                         vehicle["statistics"]["overall"]["overall_max_speed_kmh"] = round(stats.overall_max_speed)
                    vehicle["statistics"]["overall"]["countries"] = ", ".join(sorted_countries) if sorted_countries else "N/A"

                    if total_distance > 0:
                        vehicle["statistics"]["overall"]["ev_ratio_percent"] = round((total_ev_distance / total_distance) * 100, 1)
                        vehicle["statistics"]["overall"]["highway_ratio_percent"] = round((total_highway_distance / total_distance) * 100, 1)
                    else:
                         vehicle["statistics"]["overall"]["highway_ratio_percent"] = 0


                    if total_distance > 0 and total_fuel > 0:
                        vehicle["statistics"]["overall"]["fuel_consumption_l_100km"] = round((total_fuel / total_distance) * 100, 2)
                    _LOGGER.debug(f"Final overall stats object: {vehicle['statistics']['overall']}")
                else:
                    _LOGGER.debug("No trip data found for this VIN, skipping overall stats calculation.")
        finally:
            db.close()

        return vehicles_data

async def log_stream_generator(request: Request):
    """Yields historical and then live log messages as Server-Sent Events."""
    # Send the recent history to the new client
    for log_entry in list(log_history):
        if await request.is_disconnected():
            break
        yield f"data: {json.dumps(log_entry)}\n\n"
    
    # Now, stream new logs as they arrive in the queue
    while True:
        if await request.is_disconnected():
            break
        try:
            log_entry = await asyncio.wait_for(log_queue.get(), timeout=30)
            yield f"data: {json.dumps(log_entry)}\n\n"
            log_queue.task_done()
        except asyncio.TimeoutError:
            yield ": keep-alive\n\n"

@app.get("/api/logs")
async def stream_logs(request: Request):
    """API endpoint to stream logs using Server-Sent Events (SSE)."""
    return StreamingResponse(log_stream_generator(request), media_type="text/event-stream")

@app.get("/api/vehicles/{vin}/history")
def get_vehicle_history(vin: str, days: int = 30):
    """API endpoint to get historical data for a vehicle."""
    db = database.SessionLocal()
    try:
        start_date = datetime.datetime.utcnow() - datetime.timedelta(days=days)
        readings = db.query(database.VehicleReading).filter(
            database.VehicleReading.vin == vin,
            database.VehicleReading.timestamp >= start_date
        ).order_by(database.VehicleReading.timestamp.asc()).all()
        return readings
    finally:
        db.close()

@app.get("/api/vehicles/{vin}/daily_summary")
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
        earliest_trip_ts = db.query(func.min(database.Trip.start_timestamp)).filter(database.Trip.vin == vin).scalar()

        if not earliest_trip_ts:
            _LOGGER.info(f"No trip data found for VIN {vin}. Returning empty daily summary.")
            return []

        # Determine the start date for the query filter.
        actual_start_date_filter = earliest_trip_ts
        if days is not None:
            # If a specific period is requested, find the later of the two dates.
            requested_start_date = datetime.datetime.utcnow() - datetime.timedelta(days=days)
            actual_start_date_filter = max(earliest_trip_ts, requested_start_date)

        # Build the main query for trips within the determined date range.
        trips_query = db.query(
            func.date(database.Trip.start_timestamp).label("day"),
            func.sum(database.Trip.distance_km).label("distance"),
            func.sum(database.Trip.fuel_consumption_l_100km * database.Trip.distance_km / 100).label("fuel"),
            func.sum(database.Trip.ev_distance_km).label("ev_distance"),
            func.sum(database.Trip.ev_duration_seconds).label("ev_duration"),
            func.avg(database.Trip.score_global).label("avg_score"),
            func.sum(database.Trip.duration_seconds).label("total_duration"),
            func.max(database.Trip.max_speed_kmh).label("max_speed")
        ).filter(
            database.Trip.vin == vin,
            database.Trip.start_timestamp >= actual_start_date_filter
        ).group_by(func.date(database.Trip.start_timestamp)).all()

        # Create a dictionary with default zero values for every day in the date range.
        daily_data = {}
        start_date_for_range = actual_start_date_filter.date()
        end_date_for_range = datetime.datetime.utcnow().date()
        num_days_in_range = (end_date_for_range - start_date_for_range).days + 1
        
        if num_days_in_range > 0:
            for i in range(num_days_in_range):
                current_date = start_date_for_range + datetime.timedelta(days=i)
                daily_data[current_date.isoformat()] = {
                    "distance": 0.0, "fuel": 0.0, "ev_distance": 0.0, 
                    "ev_duration": 0, "score": None, "duration_seconds": 0, "max_speed": None
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
                "fuel_consumption_l_100km": round((data["fuel"] / data["distance"]) * 100, 2) if data["fuel"] > 0 and data["distance"] > 0 else 0.0,
                "ev_distance_km": round(data.get("ev_distance", 0), 2),
                "ev_duration_seconds": data.get("ev_duration", 0),
                "score_global": round(data["score"], 0) if data.get("score") is not None else None,
                "duration_seconds": data.get("duration_seconds", 0),
                "average_speed_kmh": round(data["distance"] / (data["duration_seconds"] / 3600), 2) if data.get("duration_seconds", 0) > 0 and data["distance"] > 0 else 0.0,
                "max_speed_kmh": data.get("max_speed")
            }
            for day, data in sorted(daily_data.items())
        ]
    finally:
        db.close()

@app.get("/api/vehicles/{vin}/trip_count")
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
            return {"trip_count": 0} # Should not happen with current UI

        # Find the absolute earliest trip for this VIN to use as a boundary.
        earliest_trip_ts = db.query(func.min(database.Trip.start_timestamp)).filter(database.Trip.vin == vin).scalar()

        if not earliest_trip_ts:
            return {"trip_count": 0}

        # Determine the start date for the query filter.
        start_date_filter = earliest_trip_ts
        if days is not None:
            requested_start_date = datetime.datetime.utcnow() - datetime.timedelta(days=days)
            start_date_filter = max(earliest_trip_ts, requested_start_date)

        # Perform the count query
        count = db.query(database.Trip).filter(
            database.Trip.vin == vin,
            database.Trip.start_timestamp >= start_date_filter
        ).count()
        
        return {"trip_count": count}
    finally:
        db.close()

@app.get("/api/geocode_status")
def get_geocode_status():
    """API endpoint to get the number of trips pending geocoding."""
    db = database.SessionLocal()
    try:
        pending_count = db.query(database.Trip).filter(database.Trip.start_address == "Geocoding...").count()
        total_count = db.query(database.Trip).count()
        return {"pending": pending_count, "total": total_count}
    finally:
        db.close()

@app.get("/api/vehicles/{vin}/countries")
def get_available_countries(vin: str):
    """Gets a unique, sorted list of country codes for all trips for a given VIN."""
    db = database.SessionLocal()
    try:
        results = db.query(database.Trip.countries).filter(
            database.Trip.vin == vin,
            database.Trip.countries.is_not(None),
            database.Trip.countries != ''
        ).distinct().all()
        
        unique_countries = set()
        for res in results:
            if res[0]:
                unique_countries.update(res[0])
        
        return sorted(list(unique_countries))
    finally:
        db.close()

@app.get("/api/trips")
def get_trips(
    vin: str,
    sort_by: str = "start_timestamp",
    sort_direction: str = "desc",
    unit_system: str = "metric",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    countries: Optional[str] = Query(None)
):
    """API endpoint to get all imported trips for a vehicle, with date and country filtering."""
    db = database.SessionLocal()
    try:
        from sqlalchemy import text

        valid_sort_columns = {c.name for c in database.Trip.__table__.columns}
        if sort_by not in valid_sort_columns:
            raise HTTPException(status_code=400, detail=f"Invalid sort_by parameter.")

        sort_column_name = {
            "distance_km": "distance_mi" if unit_system.startswith('imperial') else "distance_km",
            "fuel_consumption_l_100km": "mpg_uk" if unit_system == 'imperial_uk' else ("mpg" if unit_system == 'imperial_us' else "fuel_consumption_l_100km"),
            "average_speed_kmh": "average_speed_mph" if unit_system.startswith('imperial') else "average_speed_kmh",
            "ev_distance_km": "ev_distance_mi" if unit_system.startswith('imperial') else "ev_distance_km",
        }.get(sort_by, sort_by)

        sort_expression = None
        if sort_by == "fuel_consumption_l_100km":
            if unit_system.startswith('imperial'):
                if sort_direction == "desc":
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL OR {sort_column_name} = 0 THEN 1 ELSE 0 END, {sort_column_name} DESC")
                else:
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL OR {sort_column_name} = 0 THEN 0 ELSE 1 END, {sort_column_name} ASC")
            else:
                if sort_direction == "desc":
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL THEN 1 ELSE 0 END, {sort_column_name} ASC")
                else:
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL THEN 1 ELSE 0 END, {sort_column_name} DESC")
        else:
            direction_sql = "DESC" if sort_direction == "desc" else "ASC"
            sort_expression = text(f"{sort_column_name} {direction_sql} NULLS LAST")

        # Base query
        query = db.query(database.Trip).options(defer(database.Trip.route))
        query = query.filter(database.Trip.vin == vin)

        # Apply date filters if provided
        if start_date:
            try:
                start_dt = datetime.datetime.fromisoformat(start_date).replace(hour=0, minute=0, second=0)
                query = query.filter(database.Trip.start_timestamp >= start_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD.")
        if end_date:
            try:
                end_dt = datetime.datetime.fromisoformat(end_date).replace(hour=23, minute=59, second=59)
                query = query.filter(database.Trip.start_timestamp <= end_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD.")

        # Apply country filter if provided
        if countries:
            country_list = [c.strip() for c in countries.split(',') if c.strip()]
            if country_list:
                country_filters = [func.instr(database.Trip.countries, f'"{country}"') > 0 for country in country_list]
                query = query.filter(or_(*country_filters))

        # Apply sorting and fetch all results
        trips = query.order_by(sort_expression).all()

        # This prevents "N/A" on the frontend if the backfill hasn't run for new trips.
        if unit_system.startswith('imperial'):
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

@app.get("/api/vehicles/{vin}/trip_data")
def get_trip_data(vin: str, period: str = "30", metric: str = "fuel_consumption_l_100km"):
    """
    API endpoint to get a raw list of a single metric's values from all individual trips in a period.
    """
    # Validate the requested metric against the Trip model to ensure it's a safe, valid column.
    valid_metrics = [c.name for c in database.Trip.__table__.columns]
    if metric not in valid_metrics:
        raise HTTPException(status_code=400, detail=f"Invalid metric specified: {metric}")

    db = database.SessionLocal()
    try:
        days: Optional[int] = None
        if period.isdigit():
            days = int(period)
        elif period != "all":
            return {"values": []}

        earliest_trip_ts = db.query(func.min(database.Trip.start_timestamp)).filter(database.Trip.vin == vin).scalar()
        if not earliest_trip_ts:
            return {"values": []}

        start_date_filter = earliest_trip_ts
        if days is not None:
            requested_start_date = datetime.datetime.utcnow() - datetime.timedelta(days=days)
            start_date_filter = max(earliest_trip_ts, requested_start_date)

        # Query for the single column of data.
        query_result = db.query(getattr(database.Trip, metric)).filter(
            database.Trip.vin == vin,
            database.Trip.start_timestamp >= start_date_filter
        ).all()
        
        # The result is a list of tuples, e.g., [(5.5,), (6.1,)]. This flattens it to [5.5, 6.1].
        values = [item[0] for item in query_result if item[0] is not None]
        
        return {"values": values}
    finally:
        db.close()

# Fetch the route for a single trip on demand
@app.get("/api/trips/{trip_id}/route")
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

@app.get("/api/vehicles/{vin}/heatmap")
def get_heatmap_data(vin: str):
    """
    Fetches all GPS route points for a vehicle to generate a heatmap.
    """
    db = database.SessionLocal()
    try:
        # Query for all trips for the given VIN that have route data
        trips_with_routes = db.query(database.Trip.route).filter(
            database.Trip.vin == vin,
            database.Trip.route != None
        ).all()

        all_points = []
        for trip_route in trips_with_routes:
            # The route is stored as a list of points in the first element of the tuple
            route_points = trip_route[0]
            if isinstance(route_points, list):
                for point in route_points:
                    # Add each point as a [lat, lon] list
                    if isinstance(point, dict) and 'lat' in point and 'lon' in point:
                        all_points.append([point['lat'], point['lon']])
        
        _LOGGER.info(f"Returning {len(all_points)} points for VIN {vin} heatmap.")
        return all_points
    finally:
        db.close()

@app.post("/api/force_poll")
async def force_poll():
    """Manually triggers a data fetch."""
    try:
        logging.info("Manual poll triggered via API.")
        await fetcher.run_fetch_cycle()
        return {"message": "Data poll completed successfully."}
    except Exception as e:
        logging.error(f"Error during manual poll: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during the data poll.")

@app.post("/api/mqtt/test")
async def mqtt_test():
    """
    Sends the latest cached data to the MQTT broker for testing purposes.
    """
    _LOGGER.info("MQTT test message triggered via API.")
    
    vehicles = await get_cached_vehicle_data()
    if not vehicles:
        raise HTTPException(status_code=404, detail="No cached vehicle data found. Please run a poll first.")

    mqtt_client = mqtt.get_client()
    if not mqtt_client:
        raise HTTPException(status_code=400, detail="MQTT is not enabled or configured correctly. Please check settings.")

    try:
        for vehicle in vehicles:
            mqtt.publish_autodiscovery_configs(mqtt_client, vehicle)
            mqtt.publish_vehicle_data(mqtt_client, vehicle)
        return {"message": "Test message sent successfully to MQTT broker."}
    except Exception as e:
        _LOGGER.error(f"Error during MQTT test publish: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while sending the MQTT message.")
    finally:
        if mqtt_client:
            mqtt.disconnect(mqtt_client)

@app.get("/api/credentials")
def get_stored_username():
    """API endpoint to get the stored username."""
    username = get_username()
    return {"username": username or ""}

@app.post("/api/credentials")
def update_credentials(creds: dict = Body(...)):
    """API endpoint to update and save credentials."""
    username = creds.get("username")
    password = creds.get("password")
    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password are required.")
    try:
        save_credentials(username, password)
        return {"message": "Credentials saved successfully."}
    except Exception as e:
        logging.error(f"Error saving credentials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to save credentials.")
@app.get("/api/config")
def get_config():
    """API endpoint to get the current configuration."""
    return settings

def deep_merge(source, destination):
    """Recursively merge dictionaries. User settings (source) overwrite defaults (destination)."""
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            deep_merge(value, node)
        else:
            destination[key] = value
    return destination

@app.post("/api/config")
def update_config(new_settings: dict = Body(...)):
    """API endpoint to update and save configuration to the user-specific config file."""
    try:
        # 1. Read the existing user config to preserve unchanged settings
        try:
            with open(USER_CONFIG_PATH, 'r') as f:
                current_user_config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            current_user_config = {}

        # 2. Deep merge the new settings from the UI into the existing user settings
        updated_user_config = deep_merge(new_settings, current_user_config)

        # 3. Write the result back to user_config.yaml
        with open(USER_CONFIG_PATH, 'w') as f:
            yaml.dump(updated_user_config, f, default_flow_style=False, sort_keys=False)

        # 4. Reload the configuration into memory for the running app
        load_config()

        return {"message": "Settings saved successfully."}
    except Exception as e:
        _LOGGER.error(f"Error updating user config file: {e}")
        raise HTTPException(status_code=500, detail="Failed to write to user configuration file.")


@app.post("/api/vehicles/{vin}/fetch_trips")
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
        logging.error(f"Error during manual trip backfill for VIN {vin}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during the trip fetch.")

@app.post("/api/import/trips")
async def import_trips_from_csv(file: UploadFile = File(...)):
    """
    Imports historical trip data from a CSV file exported from the Toyota app.
    The filename is expected to contain the VIN (e.g., 'VIN_YYYY-MM-DD_YYYY-MM-DD.csv').
    """
    filename = file.filename
    try:
        vin = filename.split('_')[0]
        if not (vin.startswith("SB") or vin.startswith("JT")) or len(vin) < 17: # Basic VIN check
             raise ValueError("Filename does not appear to contain a valid VIN.")
    except (IndexError, ValueError) as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid filename format. Expected 'VIN_start-date_end-date.csv'. Error: {e}"
        )

    content = await file.read()
    content_text = content.decode('utf-8')
    file_like_object = io.StringIO(content_text)
    reader = csv.reader(file_like_object, delimiter=';')
    
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
                distance_csv = float(row[4].replace(',', '.'))
                start_ts_utc = datetime.datetime.fromisoformat(row[1]).astimezone(datetime.timezone.utc)
                end_ts_utc = datetime.datetime.fromisoformat(row[3]).astimezone(datetime.timezone.utc)
                fuel_consumption_csv = float(row[5].replace(',', '.'))

                # --- Content-Based Deduplication Logic ---
                # Find a trip with the same addresses and a very similar distance.
                distance_tolerance = 0.1  # 100 meters tolerance for small variations

                existing_trip = db.query(database.Trip).filter(
                    database.Trip.vin == vin,
                    database.Trip.start_address == start_address_csv,
                    database.Trip.end_address == end_address_csv,
                    database.Trip.distance_km.between(distance_csv - distance_tolerance, distance_csv + distance_tolerance)
                ).first()

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
                        fuel_consumption_l_100km=fuel_consumption_csv
                    )
                    db.add(new_trip)
                    imported_count += 1
            except (ValueError, IndexError):
                skipped_count += 1
        
        db.commit() # Commit the entire transaction once at the end.
        return {"message": "Import complete.", "imported": imported_count, "updated": updated_count, "skipped_duplicates_or_errors": skipped_count}
    except Exception as e:
        db.rollback()
        logging.error(f"Error during CSV import transaction: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="A critical error occurred during import. The entire operation was rolled back.")
    finally:
        db.close()

@app.post("/api/backfill_geocoding")
async def trigger_geocoding_backfill():
    """Triggers a manual, on-demand backfill of missing geocoding data."""
    try:
        result = await fetcher.backfill_geocoding()
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except Exception as e:
        logging.error(f"Error during manual geocoding backfill: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during the geocoding backfill.")

@app.post("/api/backfill_units")
def backfill_imperial_units():
    """One-off utility to calculate and save imperial units for all existing trips."""
    db = database.SessionLocal()
    try:
        _LOGGER.info("Starting backfill process for imperial units...")
        # Find all trips where UK MPG hasn't been calculated yet
        trips_to_update = db.query(database.Trip).filter(database.Trip.mpg_uk == None).all()
        
        if not trips_to_update:
            return {"message": "No trips needed backfilling. All data is up to date."}

        KM_TO_MI = 0.621371
        for trip in trips_to_update:
            if trip.distance_km is not None:
                trip.distance_mi = trip.distance_km * KM_TO_MI
            if trip.ev_distance_km is not None:
                trip.ev_distance_mi = trip.ev_distance_km * KM_TO_MI
            if trip.average_speed_kmh is not None:
                trip.average_speed_mph = trip.average_speed_kmh * KM_TO_MI
            trip.mpg = (235.214 / trip.fuel_consumption_l_100km) if trip.fuel_consumption_l_100km and trip.fuel_consumption_l_100km > 0 else 0.0
            trip.mpg_uk = (282.481 / trip.fuel_consumption_l_100km) if trip.fuel_consumption_l_100km and trip.fuel_consumption_l_100km > 0 else 0.0
        
        db.commit()
        _LOGGER.info(f"Successfully backfilled imperial units for {len(trips_to_update)} trips.")
        return {"message": f"Successfully backfilled imperial units for {len(trips_to_update)} trips."}
    except Exception as e:
        db.rollback()
        _LOGGER.error(f"Error during imperial unit backfill: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred during the backfill process.")
    finally:
        db.close()

@app.post("/api/vehicles/{vin}/service_history")
async def trigger_service_history_fetch(vin: str):
    """Fetches service history and updates the vehicle cache."""
    history_data = await fetcher.fetch_service_history(vin=vin)
    if "error" in history_data:
        raise HTTPException(status_code=500, detail=history_data["error"])

    async with fetcher.CACHE_LOCK:
        try:
            async with aiofiles.open(fetcher.CACHE_FILE, 'r') as f:
                content = await f.read()
                data = json.loads(content)
        except (IOError, json.JSONDecodeError):
            _LOGGER.warning("Could not open cache file to save service history, returning live data only.")
            return history_data

        vehicle_found = False
        for vehicle in data.get("vehicles", []):
            if vehicle.get("vin") == vin:
                vehicle["service_history"] = history_data.get("service_histories", [])
                vehicle_found = True
                break
        
        if not vehicle_found:
             _LOGGER.warning(f"VIN {vin} not found in cache file. Unable to save service history.")
             return history_data

        try:
            CACHE_FILE_TMP = fetcher.CACHE_FILE.with_suffix(".tmp")
            async with aiofiles.open(CACHE_FILE_TMP, "w") as f:
                await f.write(json.dumps(data, indent=2))
            await aiofiles.os.replace(CACHE_FILE_TMP, fetcher.CACHE_FILE)
            _LOGGER.info(f"Successfully fetched and saved service history for VIN {vin}.")
        except IOError as e:
            _LOGGER.error(f"Failed to write updated cache file with service history: {e}")

    return history_data