# app/fetcher.py
import asyncio
import json
import os
import datetime
import logging
from pathlib import Path

import aiofiles
import aiofiles.os
from pytoyoda.client import MyT
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from pytoyoda.exceptions import ToyotaLoginError, ToyotaApiError
from . import database
from .credentials_manager import load_credentials
from sqlalchemy.exc import IntegrityError

from .config import settings, DATA_DIR

_LOGGER = logging.getLogger(__name__)

CACHE_FILE = DATA_DIR / "vehicle_data.json"

# Create a lock to manage access to the cache file to prevent race conditions
CACHE_LOCK = asyncio.Lock()

async def _fetch_and_process_trip_summaries(vehicle, db_session, from_date, to_date):
    """Helper function to fetch and save trip summaries for a given period."""
    _LOGGER.info(f"Fetching trip summaries for VIN {vehicle.vin} from {from_date} to {to_date}...")

    # Initialize the geocoder with a rate limiter to respect Nominatim's usage policy.
    geolocator = Nominatim(user_agent="mytoyota_dashboard", timeout=10)
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1.1, return_value_on_exception=None)

    # Per your feedback, we call get_trips without the 'summary' parameter.
    # This returns a list of summary objects directly.
    all_trips = await vehicle.get_trips(from_date=from_date, to_date=to_date)

    if not isinstance(all_trips, list):
        _LOGGER.error(f"Expected a list of trips, but got {type(all_trips)}. Aborting trip fetch.")
        return {"new": 0, "updated": 0, "skipped": 0, "error": "Invalid response from API library"}

    _LOGGER.info(f"API returned a total of {len(all_trips)} trips.")

    new_trips_count = 0
    updated_trips_count = 0
    skipped_trips_count = 0

    for trip in all_trips:
        try:
            # The trip object's coordinates are nested. We check for the nested attribute.
            if not (hasattr(trip, 'locations') and hasattr(trip.locations, 'start') and hasattr(trip.locations.start, 'lat')):
                _LOGGER.warning(f"Skipping a trip object because it's missing coordinate data.")
                continue

            # Use start_time for a more reliable primary check to avoid unnecessary processing.
            start_ts_utc = trip.start_time.astimezone(datetime.timezone.utc)
            
            existing_trip = db_session.query(database.Trip).filter(
                database.Trip.vin == vehicle.vin,
                database.Trip.start_timestamp == start_ts_utc
            ).first()

            # If trip exists and is already complete (has coordinates), skip it.
            if existing_trip and existing_trip.start_lat is not None:
                skipped_trips_count += 1
                continue

            # If we are here, the trip is either new or needs to be updated.
            # Now we can perform the expensive geocoding call.
            start_location = reverse(f"{trip.locations.start.lat}, {trip.locations.start.lon}")
            end_location = reverse(f"{trip.locations.end.lat}, {trip.locations.end.lon}")
            start_address = start_location.address if start_location else "Unknown"
            end_address = end_location.address if end_location else "Unknown"

            # Extract and convert primary metrics
            distance = trip.distance if hasattr(trip, 'distance') and trip.distance is not None else 0.0
            fuel_consumption_l_100km = trip.average_fuel_consumed if hasattr(trip, 'average_fuel_consumed') and trip.average_fuel_consumed is not None else 0.0

            # Extract detailed metrics, with fallbacks for missing data
            duration_seconds = trip.duration.total_seconds() if hasattr(trip, 'duration') and trip.duration else 0
            average_speed_kmh = (distance / (duration_seconds / 3600)) if duration_seconds > 0 and distance > 0 else 0.0
            max_speed_kmh = None  # Not available in this summary object
            ev_distance_km = trip.ev_distance if hasattr(trip, 'ev_distance') and trip.ev_distance is not None else 0.0
            ev_duration_seconds = trip.ev_duration.total_seconds() if hasattr(trip, 'ev_duration') and trip.ev_duration else 0
            score_acceleration = None
            score_braking = None
            score_global = trip.score if hasattr(trip, 'score') else None

            if existing_trip: # This means it was incomplete (e.g., from CSV import)
                existing_trip.start_address = start_address
                existing_trip.end_address = end_address
                existing_trip.start_lat = trip.locations.start.lat
                existing_trip.start_lon = trip.locations.start.lon
                existing_trip.end_lat = trip.locations.end.lat
                existing_trip.end_lon = trip.locations.end.lon
                existing_trip.duration_seconds = duration_seconds
                existing_trip.average_speed_kmh = average_speed_kmh
                existing_trip.max_speed_kmh = max_speed_kmh
                existing_trip.ev_distance_km = ev_distance_km
                existing_trip.ev_duration_seconds = ev_duration_seconds
                existing_trip.score_global = score_global
                updated_trips_count += 1
            else: # This is a brand new trip
                new_trip = database.Trip(
                    vin=vehicle.vin,
                    start_timestamp=start_ts_utc,
                    end_timestamp=trip.end_time.astimezone(datetime.timezone.utc),
                    start_address=start_address,
                    start_lat=trip.locations.start.lat,
                    start_lon=trip.locations.start.lon,
                    end_address=end_address,
                    end_lat=trip.locations.end.lat,
                    end_lon=trip.locations.end.lon,
                    distance_km=distance,
                    fuel_consumption_l_100km=fuel_consumption_l_100km,
                    duration_seconds=duration_seconds,
                    average_speed_kmh=average_speed_kmh,
                    max_speed_kmh=max_speed_kmh,
                    ev_distance_km=ev_distance_km,
                    ev_duration_seconds=ev_duration_seconds,
                    score_acceleration=score_acceleration,
                    score_braking=score_braking,
                    score_global=score_global
                )
                db_session.add(new_trip)
                new_trips_count += 1
        except Exception as e:
            _LOGGER.warning(f"Could not process a trip summary due to an error: {e}. Skipping.", exc_info=True)
            db_session.rollback()

    if new_trips_count > 0 or updated_trips_count > 0:
        db_session.commit()

    _LOGGER.info(f"Trip summary fetch for {vehicle.vin} complete. New: {new_trips_count}, Updated: {updated_trips_count}, Skipped: {skipped_trips_count}.")
    return {"new": new_trips_count, "updated": updated_trips_count, "skipped": skipped_trips_count}

async def _update_vehicle_statistics(vehicle, vehicle_info_dict):
    """
    Fetches and processes daily driving statistics for a vehicle.
    This is kept separate from the live status update for clarity.
    """
    _LOGGER.info(f"Fetching daily statistics for VIN {vehicle.vin}...")

    # Helper to process statistics
    async def process_stats(stats_obj):
        if not stats_obj:
            return None
        
        dist = stats_obj.distance if stats_obj.distance is not None else 0.0
        fuel = stats_obj.fuel_consumed if stats_obj.fuel_consumed is not None else 0.0
        ev_dist = stats_obj.ev_distance if stats_obj.ev_distance is not None else 0.0

        fuel_consumption = None
        non_ev_dist = dist - ev_dist
        distance_for_fuel_calc = non_ev_dist if vehicle_info_dict["is_hybrid"] and non_ev_dist > 0 else dist

        if fuel > 0 and distance_for_fuel_calc > 0:
            fuel_consumption = round((fuel / distance_for_fuel_calc) * 100, 2)
        else:
            fuel_consumption = 0.0

        return {
            "distance": dist,
            "fuel_consumed": fuel,
            "calculated_fuel_consumption_l_100km": fuel_consumption,
        }

    daily_summary = await vehicle.get_current_day_summary()
    vehicle_info_dict["statistics"]["daily"] = await process_stats(daily_summary)

async def fetch_and_save_data():
    """Fetches vehicle data from the Toyota API using the pytoyoda library and saves it."""
    _LOGGER.info("Starting data fetch process with pytoyoda library...")

    username, password = load_credentials()

    if not username or not password:
        _LOGGER.error("Credentials not found. Please set them on the Settings page.")
        return

    client = MyT(username=username, password=password, use_metric=True)
    all_vehicle_data = []

    try:
        _LOGGER.info("Attempting to log in...")
        await client.login()
        _LOGGER.info("Login successful!")

        vehicles = await client.get_vehicles()
        if not vehicles:
            _LOGGER.info("No vehicles found for this account.")
            return

        _LOGGER.info(f"Found {len(vehicles)} vehicle(s). Processing...")

        for vehicle in vehicles:
            if not vehicle or not vehicle.vin:
                _LOGGER.warning("Skipping a vehicle due to missing data or VIN.")
                continue

            # Use retry logic from your working script
            api_retries = settings.get("api_retries", 3)
            api_retry_delay = settings.get("api_retry_delay_seconds", 5)
            try:
                for attempt in range(api_retries + 1):
                    try:
                        _LOGGER.info(f"Updating dashboard for VIN {vehicle.vin} (Attempt {attempt + 1}/{api_retries + 1})")
                        await vehicle.update()
                        _LOGGER.info(f"Data updated for VIN: {vehicle.vin}")
                        break 
                    except ToyotaApiError as e:
                        _LOGGER.warning(f"API error during vehicle.update() for VIN {vehicle.vin}: {e}")
                        if attempt < api_retries:
                            _LOGGER.info(f"Retrying in {api_retry_delay} seconds...")
                            await asyncio.sleep(api_retry_delay)
                        else:
                            _LOGGER.error(f"Failed to update vehicle {vehicle.vin} after all retries.")
                            raise

                # Build a comprehensive vehicle data dictionary, similar to the `mytoyota` library's output
                vehicle_info = {
                    "vin": vehicle.vin,
                    "alias": vehicle.alias or "N/A",
                    "is_hybrid": vehicle.type in ["hybrid", "phev"],
                    "model_name": getattr(vehicle._vehicle_info, "car_model_name", None),
                    "dashboard": {},
                    "statistics": {},
                    "status": {}
                }

                if vehicle.dashboard:
                    dashboard = vehicle.dashboard
                    vehicle_info["dashboard"] = {
                        "odometer": getattr(dashboard, "odometer", None),
                        "fuel_level": getattr(dashboard, "fuel_level", None),
                        "total_range": getattr(dashboard, "range", None), # 'range' in pytoyoda is total_range
                        "fuel_range": getattr(dashboard, "fuel_range", None),
                        "battery_level": getattr(dashboard, "battery_level", None),
                        "battery_range": getattr(dashboard, "battery_range", None),
                    }
                
                # Add location data if available
                if hasattr(vehicle, 'location') and vehicle.location:
                    vehicle_info["dashboard"]["latitude"] = getattr(vehicle.location, 'latitude', None)
                    vehicle_info["dashboard"]["longitude"] = getattr(vehicle.location, 'longitude', None)

                # The pytoyoda library provides status information under the `lock_status` attribute.
                doors_status = {}
                windows_status = {}
                hood_closed = None
                trunk_closed = None
                trunk_locked = None
                last_update_timestamp = None

                if hasattr(vehicle, 'lock_status') and vehicle.lock_status:
                    lock_status = vehicle.lock_status
                    if hasattr(lock_status, 'doors') and lock_status.doors:
                        doors = lock_status.doors
                        door_map = {
                            'driver_seat': 'front_left', 'passenger_seat': 'front_right',
                            'driver_rear_seat': 'rear_left', 'passenger_rear_seat': 'rear_right',
                        }
                        for attr_name, key in door_map.items():
                            if hasattr(doors, attr_name):
                                door_obj = getattr(doors, attr_name)
                                doors_status[key] = {"closed": door_obj.closed, "locked": door_obj.locked}
                        # The trunk is also part of the doors object according to the previous print.
                        if hasattr(doors, 'trunk'):
                            trunk_closed = doors.trunk.closed
                            trunk_locked = doors.trunk.locked

                    if hasattr(lock_status, 'windows') and lock_status.windows:
                        windows = lock_status.windows
                        window_map = {
                            'driver_seat': 'front_left', 'passenger_seat': 'front_right',
                            'driver_rear_seat': 'rear_left', 'passenger_rear_seat': 'rear_right',
                        }
                        for attr_name, key in window_map.items():
                            if hasattr(windows, attr_name):
                                window_obj = getattr(windows, attr_name)
                                windows_status[key] = {"closed": window_obj.closed}

                    if hasattr(lock_status, 'hood'):
                        hood_closed = lock_status.hood.closed
                    if hasattr(lock_status, 'last_update_timestamp'):
                        last_update_timestamp = lock_status.last_update_timestamp.isoformat() if lock_status.last_update_timestamp else None

                vehicle_info["status"] = {
                    "doors": doors_status,
                    "windows": windows_status,
                    "hood_closed": hood_closed,
                    "trunk_closed": trunk_closed,
                    "trunk_locked": trunk_locked,
                    "last_update_timestamp": last_update_timestamp
                }

                # Explicitly fetch statistics as a separate step.
                await _update_vehicle_statistics(vehicle, vehicle_info)
                
                # Add the new data point to the historical database, but only if the odometer has changed.
                latest_reading = database.get_latest_reading(vin=vehicle.vin)
                new_odometer = vehicle_info.get("dashboard", {}).get("odometer")

                if new_odometer is not None and (not latest_reading or new_odometer > latest_reading.odometer):
                    _LOGGER.info(f"New odometer reading ({new_odometer} km) for {vehicle.vin}. Adding to history.")
                    database.add_reading(vehicle_info)

                    # --- Automatic Trip Summary Fetching ---
                    # Only fetch trips if the car has moved.
                    db = database.SessionLocal()
                    try:
                        # Fetch trips from the last known trip date to catch up on any missed days.
                        to_date = datetime.date.today()
                        latest_trip_ts = database.get_latest_trip_timestamp(vin=vehicle.vin)
                        
                        if latest_trip_ts:
                            # Start fetching from the day of the last trip.
                            from_date = latest_trip_ts.date()
                        else:
                            # If no trips exist, fetch the last 7 days as a reasonable default.
                            _LOGGER.info(f"No existing trips found for {vehicle.vin}. Fetching last 7 days as a default.")
                            from_date = to_date - datetime.timedelta(days=7)

                        _LOGGER.info(f"Odometer changed, automatically fetching recent trips from {from_date} to {to_date}.")
                        await _fetch_and_process_trip_summaries(vehicle, db, from_date, to_date)
                    finally:
                        db.close()
                else:
                    _LOGGER.info(f"Odometer for {vehicle.vin} has not changed. Skipping database entry and trip fetch.")

                all_vehicle_data.append(vehicle_info)

            except ToyotaApiError as e:
                _LOGGER.error(f"API error processing vehicle {vehicle.vin}: {e}")
            except Exception as e:
                _LOGGER.error(f"Unexpected error with vehicle {vehicle.vin}: {e}", exc_info=True)

        # --- Safe File Write Logic ---
        # Save the successfully processed data to a temporary file first to prevent race conditions.
        CACHE_FILE_TMP = CACHE_FILE.with_suffix(f"{CACHE_FILE.suffix}.tmp")
        async with CACHE_LOCK:
            try:
                if all_vehicle_data:
                    async with aiofiles.open(CACHE_FILE_TMP, "w") as f:
                        await f.write(json.dumps(all_vehicle_data, indent=2))
                    
                    # Atomically replace the destination file with the temporary one.
                    await aiofiles.os.replace(CACHE_FILE_TMP, CACHE_FILE)
                    _LOGGER.info(f"Successfully fetched and saved data for {len(all_vehicle_data)} vehicle(s).")
                else:
                    _LOGGER.info("No new vehicle data processed, cache file not updated.")
            except Exception as e:
                _LOGGER.error(f"Failed to write vehicle data cache file: {e}", exc_info=True)
                if await aiofiles.os.path.exists(CACHE_FILE_TMP):
                    await aiofiles.os.remove(CACHE_FILE_TMP)

    except ToyotaLoginError as e:
        _LOGGER.error(f"Login failed: {e}")
    except ToyotaApiError as e:
        _LOGGER.error(f"API error during initial fetch: {e}")
    except Exception as e:
        _LOGGER.error(f"An unexpected error occurred in fetcher: {e}", exc_info=True)
    finally:
        if client and hasattr(client, "_session") and client._session and not client._session.is_closed:
            _LOGGER.info("Closing pytoyoda client session.")
            await client._session.aclose()

async def backfill_trips(vin: str, period: str):
    """Logs in and fetches historical trips for a specific vehicle and period."""
    _LOGGER.info(f"Starting manual trip backfill for VIN {vin}, period: {period}")
    username, password = load_credentials()

    if not username or not password:
        return {"error": "Credentials not found."}

    client = MyT(username=username, password=password, use_metric=True)
    try:
        await client.login()
        vehicles = await client.get_vehicles()
        target_vehicle = next((v for v in vehicles if v.vin == vin), None)

        if not target_vehicle:
            return {"error": f"Vehicle with VIN {vin} not found on this account."}

        to_date = datetime.date.today()
        if period == "week":
            from_date = to_date - datetime.timedelta(days=7)
        elif period == "month":
            from_date = to_date - datetime.timedelta(days=31)
        elif period == "year":
            from_date = to_date - datetime.timedelta(days=365)
        elif period == "all":
            # A reasonable limit to avoid excessively large requests
            from_date = to_date - datetime.timedelta(days=365 * 5)
        else:
            return {"error": "Invalid period specified."}

        db = database.SessionLocal()
        try:
            result = await _fetch_and_process_trip_summaries(target_vehicle, db, from_date, to_date)
            return {"message": f"Fetch for '{period}' complete.", **result}
        finally:
            db.close()
    except Exception as e:
        _LOGGER.error(f"Error during trip backfill: {e}", exc_info=True)
        return {"error": "An internal error occurred during the fetch."}
    finally:
        if client and hasattr(client, "_session") and client._session and not client._session.is_closed:
            await client._session.aclose()

async def fetch_and_save_status_only():
    """
    Fetches only the live vehicle status (dashboard, location, doors, etc.)
    and updates the existing cache file without re-fetching trip statistics.
    """
    _LOGGER.info("Starting status-only data fetch process...")
    
    async with CACHE_LOCK:
        if not await aiofiles.os.path.exists(CACHE_FILE):
            _LOGGER.warning("Cache file not found. Running a full data fetch instead.")
            await fetch_and_save_data()
            return

        # Read the existing cached data
        async with aiofiles.open(CACHE_FILE, 'r') as f:
            content = await f.read()
            cached_data = json.loads(content)
            cached_vehicles_map = {v.get("vin"): v for v in cached_data if v.get("vin")}

        username, password = load_credentials()

        if not username or not password:
            _LOGGER.error("Credentials not found for status-only poll.")
            return

        client = MyT(username=username, password=password, use_metric=True)
        try:
            await client.login()
            vehicles = await client.get_vehicles()
            if not vehicles:
                _LOGGER.info("No vehicles found for this account during status poll.")
                return

            for vehicle in vehicles:
                if not vehicle or not vehicle.vin or vehicle.vin not in cached_vehicles_map:
                    continue

                try:
                    _LOGGER.info(f"Updating live status for VIN {vehicle.vin}...")
                    await vehicle.update()
                    _LOGGER.info(f"Live status updated for VIN: {vehicle.vin}")

                    target_vehicle_data = cached_vehicles_map[vehicle.vin]

                    # Update dashboard data
                    if vehicle.dashboard:
                        target_vehicle_data["dashboard"]["odometer"] = getattr(vehicle.dashboard, "odometer", None)
                        target_vehicle_data["dashboard"]["fuel_level"] = getattr(vehicle.dashboard, "fuel_level", None)
                        target_vehicle_data["dashboard"]["total_range"] = getattr(vehicle.dashboard, "range", None)

                    # Update location data
                    if hasattr(vehicle, 'location') and vehicle.location:
                        target_vehicle_data["dashboard"]["latitude"] = getattr(vehicle.location, 'latitude', None)
                        target_vehicle_data["dashboard"]["longitude"] = getattr(vehicle.location, 'longitude', None)

                    # Update detailed status (doors, windows, etc.)
                    if hasattr(vehicle, 'lock_status') and vehicle.lock_status:
                        ls = vehicle.lock_status
                        new_status = {"doors": {}, "windows": {}}

                        if hasattr(ls, 'doors') and ls.doors:
                            door_map = {'driver_seat': 'front_left', 'passenger_seat': 'front_right', 'driver_rear_seat': 'rear_left', 'passenger_rear_seat': 'rear_right'}
                            for attr, key in door_map.items():
                                if hasattr(ls.doors, attr):
                                    new_status["doors"][key] = {"closed": getattr(ls.doors, attr).closed, "locked": getattr(ls.doors, attr).locked}
                            if hasattr(ls.doors, 'trunk'):
                                new_status["trunk_closed"] = ls.doors.trunk.closed
                                new_status["trunk_locked"] = ls.doors.trunk.locked

                        if hasattr(ls, 'windows') and ls.windows:
                            window_map = {'driver_seat': 'front_left', 'passenger_seat': 'front_right', 'driver_rear_seat': 'rear_left', 'passenger_rear_seat': 'rear_right'}
                            for attr, key in window_map.items():
                                if hasattr(ls.windows, attr):
                                    new_status["windows"][key] = {"closed": getattr(ls.windows, attr).closed}

                        if hasattr(ls, 'hood'):
                            new_status["hood_closed"] = ls.hood.closed
                        if hasattr(ls, 'last_update_timestamp'):
                            new_status["last_update_timestamp"] = ls.last_update_timestamp.isoformat() if ls.last_update_timestamp else None
                        target_vehicle_data["status"] = new_status

                except Exception as e:
                    _LOGGER.error(f"Failed to update live status for VIN {vehicle.vin}: {e}", exc_info=True)

            # Write the modified data back to the cache using an atomic write
            updated_cache_list = list(cached_vehicles_map.values())
            CACHE_FILE_TMP = CACHE_FILE.with_suffix(f"{CACHE_FILE.suffix}.tmp")
            async with aiofiles.open(CACHE_FILE_TMP, "w") as f:
                await f.write(json.dumps(updated_cache_list, indent=2))
            await aiofiles.os.replace(CACHE_FILE_TMP, CACHE_FILE)
            _LOGGER.info("Cache updated with fresh vehicle status.")
        except Exception as e:
            _LOGGER.error(f"An unexpected error occurred in status-only fetcher: {e}", exc_info=True)
        finally:
            if client and hasattr(client, "_session") and client._session and not client._session.is_closed:
                await client._session.aclose()