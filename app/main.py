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
from typing import Deque, Dict

import aiofiles
from fastapi import FastAPI, HTTPException, Request, Body, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import fetcher
from . import database
from .credentials_manager import get_username, save_credentials
from .config import settings, load_config, CONFIG_PATH, DATA_DIR
from .logging_config import setup_logging
from sqlalchemy.exc import IntegrityError

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

@app.get("/api/vehicles")
async def get_vehicle_data():
    """API endpoint to get the cached vehicle data."""
    async with fetcher.CACHE_LOCK:
        if not fetcher.CACHE_FILE.exists():
            # Return an empty list if the cache file doesn't exist yet.
            # This provides a better frontend experience than a 404 error.
            return []
        async with aiofiles.open(fetcher.CACHE_FILE, 'r') as f:
            content = await f.read()
            data = json.loads(content)
            vehicles_data = data.get("vehicles", [])
            last_updated = data.get("last_updated", "Never")

        # Augment vehicle data with all-time statistics from the database
        db = database.SessionLocal()
        try:
            from sqlalchemy import func
            for vehicle in vehicles_data:
                if last_updated:
                    vehicle["last_updated"] = last_updated
                vin = vehicle.get("vin")
                if not vin:
                    continue
                
                stats = db.query(
                    func.sum(database.Trip.distance_km).label("total_distance"),
                    func.sum(database.Trip.ev_distance_km).label("total_ev_distance"),
                    func.sum(database.Trip.fuel_consumption_l_100km * database.Trip.distance_km / 100).label("total_fuel"),
                    func.sum(database.Trip.duration_seconds).label("total_duration_seconds")
                ).filter(database.Trip.vin == vin).first()
                
                logging.debug(f"--- Overall Stats for VIN: {vin} ---")
                logging.debug(f"Raw DB stats: total_distance={stats.total_distance}, total_ev_distance={stats.total_ev_distance}, total_fuel={stats.total_fuel}")

                vehicle["statistics"]["overall"] = {}
                if stats and stats.total_distance is not None:
                    total_distance = stats.total_distance
                    # If the sum is NULL (no EV trips), it will be None. Default to 0.
                    total_ev_distance = stats.total_ev_distance or 0.0
                    total_fuel = stats.total_fuel or 0.0
                    total_duration_seconds = stats.total_duration_seconds or 0
                    logging.debug(f"Processing stats: total_distance={total_distance}, total_ev_distance={total_ev_distance}, total_fuel={total_fuel}, total_duration={total_duration_seconds}")

                    
                    vehicle["statistics"]["overall"]["total_ev_distance_km"] = round(total_ev_distance)
                    vehicle["statistics"]["overall"]["total_fuel_l"] = round(total_fuel, 2)
                    vehicle["statistics"]["overall"]["total_duration_seconds"] = total_duration_seconds

                    if total_distance > 0:
                        vehicle["statistics"]["overall"]["ev_ratio_percent"] = round((total_ev_distance / total_distance) * 100, 1)

                    if total_distance > 0 and total_fuel > 0:
                        vehicle["statistics"]["overall"]["fuel_consumption_l_100km"] = round((total_fuel / total_distance) * 100, 2)
                    logging.debug(f"Final overall stats object: {vehicle['statistics']['overall']}")
                else:
                    logging.debug("No trip data found for this VIN, skipping overall stats calculation.")
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
def get_daily_summary(vin: str, days: int = 30):
    """
    API endpoint to get a summary of distance and fuel consumption per day,
    combining data from real-time polls and imported trips.
    """
    db = database.SessionLocal()
    try:
        from sqlalchemy import func
        start_date = datetime.datetime.utcnow() - datetime.timedelta(days=days)

        # For historical charts, we rely on the aggregated trip data as the source of truth.
        trips_query = db.query(
            func.date(database.Trip.start_timestamp).label("day"),
            func.sum(database.Trip.distance_km).label("distance"),
            func.sum(database.Trip.fuel_consumption_l_100km * database.Trip.distance_km / 100).label("fuel"),
            func.sum(database.Trip.ev_distance_km).label("ev_distance"),
            func.sum(database.Trip.ev_duration_seconds).label("ev_duration"),
            func.avg(database.Trip.score_global).label("avg_score"),
            func.sum(database.Trip.duration_seconds).label("total_duration")
        ).filter(
            database.Trip.vin == vin,
            database.Trip.start_timestamp >= start_date
        ).group_by(func.date(database.Trip.start_timestamp)).all()

        logging.debug(f"--- Daily Summary for VIN: {vin} (last {days} days) ---")
        logging.debug(f"Found {len(trips_query)} days with trip data in the database.")
        if trips_query:
            logging.debug(f"Sample trip data row: {trips_query[0]}")

        # Combine and process the results
        # First, create a dictionary with default zero values for every day in the requested range.
        # This ensures the graph has a continuous timeline.
        daily_data = {}
        # Use UTC for the end date to match the start_date's timezone basis.
        end_date = datetime.datetime.utcnow().date()
        start_date_only = start_date.date()
        num_days_in_range = (end_date - start_date_only).days + 1
        # Safeguard against any timezone-related edge cases that could make the range negative.
        if num_days_in_range < 0: num_days_in_range = 0
        for i in range(num_days_in_range):
            current_date = start_date_only + datetime.timedelta(days=i)
            day_str = current_date.isoformat()
            daily_data[day_str] = {"distance": 0.0, "fuel": 0.0, "ev_distance": 0.0, "ev_duration": 0, "score": None, "duration_seconds": 0}

        # Now, update the dictionary with the actual trip data.
        for r in trips_query:
            day_str = r.day  # func.date() with SQLite returns a string
            if day_str in daily_data:
                daily_data[day_str]["distance"] = r.distance or 0.0
                daily_data[day_str]["fuel"] = r.fuel or 0.0
                daily_data[day_str]["ev_distance"] = r.ev_distance or 0.0
                daily_data[day_str]["ev_duration"] = r.ev_duration or 0
                daily_data[day_str]["score"] = r.avg_score
                daily_data[day_str]["duration_seconds"] = r.total_duration or 0
        
        logging.debug(f"Final daily_data object contains {len(daily_data)} days before being sent to chart.")

        return [
            {
                "date": day,
                "distance_km": round(data["distance"], 2),
                "fuel_consumption_l_100km": round((data["fuel"] / data["distance"]) * 100, 2) if data["fuel"] > 0 and data["distance"] > 0 else 0.0,
                "ev_distance_km": round(data.get("ev_distance", 0), 2),
                "ev_duration_seconds": data.get("ev_duration", 0),
                "score_global": round(data["score"], 0) if data.get("score") is not None else None,
                "duration_seconds": data.get("duration_seconds", 0),
                "average_speed_kmh": round(data["distance"] / (data["duration_seconds"] / 3600), 2) if data.get("duration_seconds", 0) > 0 and data["distance"] > 0 else 0.0
            }
            for day, data in sorted(daily_data.items())
        ]
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

@app.get("/api/trips")
def get_trips(vin: str, sort_by: str = "start_timestamp", sort_direction: str = "desc", unit_system: str = "metric"):
    """API endpoint to get all imported trips for a vehicle, with robust, unit-aware server-side sorting."""
    db = database.SessionLocal()
    try:
        from sqlalchemy import text

        if sort_by not in ["start_timestamp", "distance_km", "fuel_consumption_l_100km", "duration_seconds", "score_global", "average_speed_kmh", "ev_distance_km", "ev_duration_seconds"]:
            raise HTTPException(status_code=400, detail=f"Invalid sort_by parameter.")

        # Determine the actual database column name based on the selected unit system
        sort_column_name = {
            "distance_km": "distance_mi" if unit_system.startswith('imperial') else "distance_km",
            "fuel_consumption_l_100km": "mpg_uk" if unit_system == 'imperial_uk' else ("mpg" if unit_system == 'imperial_us' else "fuel_consumption_l_100km"),
            "average_speed_kmh": "average_speed_mph" if unit_system.startswith('imperial') else "average_speed_kmh",
            "ev_distance_km": "ev_distance_mi" if unit_system.startswith('imperial') else "ev_distance_km",
        }.get(sort_by, sort_by)

        sort_expression = None
        if sort_by == "fuel_consumption_l_100km":
            if unit_system.startswith('imperial'):
                # Sorting by MPG (US or UK), higher is better. 0 is worst.
                if sort_direction == "desc":  # Best first
                    # Sort descending, but push 0s and NULLs to the end.
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL OR {sort_column_name} = 0 THEN 1 ELSE 0 END, {sort_column_name} DESC")
                else:  # Worst first
                    # Sort ascending, but push 0s and NULLs to the front.
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL OR {sort_column_name} = 0 THEN 0 ELSE 1 END, {sort_column_name} ASC")
            else:  # Metric
                # Sorting by L/100km, lower is better. 0 is best.
                if sort_direction == "desc":  # Best first
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL THEN 1 ELSE 0 END, {sort_column_name} ASC")
                else:  # Worst first
                    sort_expression = text(f"CASE WHEN {sort_column_name} IS NULL THEN 1 ELSE 0 END, {sort_column_name} DESC")
        else:
            # Standard sorting for all other columns
            direction_sql = "DESC" if sort_direction == "desc" else "ASC"
            sort_expression = text(f"{sort_column_name} {direction_sql} NULLS LAST")

        trips = db.query(database.Trip).filter(database.Trip.vin == vin).order_by(
            sort_expression
        ).all()
        return trips
    finally:
        db.close()

@app.post("/api/force_poll")
async def force_poll():
    """Manually triggers a data fetch."""
    try:
        logging.info("Manual poll triggered via API.")
        # MODIFIED: Call the new unified fetch cycle function.
        await fetcher.run_fetch_cycle()
        return {"message": "Data poll completed successfully."}
    except Exception as e:
        logging.error(f"Error during manual poll: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during the data poll.")

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

@app.post("/api/config")
def update_config(new_settings: dict = Body(...)):
    """API endpoint to update the configuration file."""
    try:
        # Read the whole config file to preserve structure and comments
        with open(CONFIG_PATH, 'r') as f:
            current_config = yaml.safe_load(f)

        # Update polling settings
        if 'polling' in new_settings.get('web_server', {}):
            current_config['web_server']['polling'] = new_settings['web_server']['polling']
            if 'data_refresh_interval_seconds' in current_config.get('web_server', {}):
                del current_config['web_server']['data_refresh_interval_seconds']

        # Update other settings
        if 'api_retries' in new_settings:
            current_config['api_retries'] = new_settings['api_retries']
        if 'api_retry_delay_seconds' in new_settings:
            current_config['api_retry_delay_seconds'] = new_settings['api_retry_delay_seconds']
        if 'unit_system' in new_settings:
            current_config['unit_system'] = new_settings['unit_system']
        if 'log_history_size' in new_settings:
            # Ensure it's a positive integer
            current_config['log_history_size'] = max(10, int(new_settings['log_history_size']))
        if 'reverse_geocode_enabled' in new_settings:
            current_config['reverse_geocode_enabled'] = new_settings['reverse_geocode_enabled']

        # Write the updated config back to the file
        with open(CONFIG_PATH, 'w') as f:
            yaml.dump(current_config, f, default_flow_style=False, sort_keys=False)

        # Reload the configuration into memory so the changes are reflected immediately.
        load_config()

        return {"message": "Settings saved successfully. Changes will be applied on the next poll."}
    except Exception as e:
        logging.error(f"Error updating config file: {e}")
        raise HTTPException(status_code=500, detail="Failed to write to configuration file.")

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