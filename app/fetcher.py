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
from . import mqtt
from .credentials_manager import load_credentials
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from .config import settings, DATA_DIR

_LOGGER = logging.getLogger(__name__)

CACHE_FILE = DATA_DIR / "vehicle_data.json"
CACHE_LOCK = asyncio.Lock()
GEOCODE_SEMAPHORE = asyncio.Semaphore(1)


async def _reverse_geocode_trip(trip_id: int):
    """Performs reverse geocoding for a specific trip, respecting the semaphore."""
    async with GEOCODE_SEMAPHORE:
        _LOGGER.info(f"Starting geocoding for trip {trip_id}...")
        db = database.SessionLocal()
        try:
            trip = db.query(database.Trip).filter(database.Trip.id == trip_id).first()
            if not trip or trip.start_address != "Geocoding...":
                _LOGGER.debug(f"Trip {trip_id} already geocoded or not found. Skipping.")
                return

            if not settings.get('reverse_geocode_enabled', True):
                trip.start_address = f"{trip.start_lat}, {trip.start_lon}"
                trip.end_address = f"{trip.end_lat}, {trip.end_lon}"
                db.commit()
                return

            loop = asyncio.get_running_loop()
            geolocator = Nominatim(user_agent="mytoyota_dashboard", timeout=10)
            reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1.1, return_value_on_exception=None)

            start_location = await loop.run_in_executor(None, reverse, f"{trip.start_lat}, {trip.start_lon}")
            end_location = await loop.run_in_executor(None, reverse, f"{trip.end_lat}, {trip.end_lon}")

            trip.start_address = start_location.address if start_location else "Unknown"
            trip.end_address = end_location.address if end_location else "Unknown"

            db.commit()
            _LOGGER.info(f"Successfully geocoded trip {trip_id}.")
        except Exception as e:
            _LOGGER.error(f"Error during background geocoding for trip {trip_id}: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()


async def _fetch_and_process_trip_summaries(vehicle, db_session, from_date, to_date):
    """Helper function to fetch, process, and save trip summaries for a given period."""
    _LOGGER.info(f"Fetching trip summaries for VIN {vehicle.vin} from {from_date} to {to_date}...")

    fetch_full_route = settings.get("fetch_full_trip_route", False)
    all_trips = await vehicle.get_trips(from_date=from_date, to_date=to_date, full_route=fetch_full_route)

    if not isinstance(all_trips, list):
        _LOGGER.error(f"Expected a list of trips, but got {type(all_trips)}. Aborting trip fetch.")
        return {"new": 0, "updated": 0, "skipped": 0, "error": "Invalid response from API library"}

    _LOGGER.info(f"API returned a total of {len(all_trips)} trips for the period.")
    new_trips_count = 0
    skipped_trips_count = 0
    updated_trips_count = 0
    
    # Define fields that should NOT be overwritten by a data backfill.
    PROTECTED_FIELDS = {'start_address', 'end_address'}

    for trip in all_trips:
        try:
            if not (hasattr(trip, 'locations') and hasattr(trip.locations, 'start') and hasattr(trip.locations.start, 'lat')):
                _LOGGER.warning("Skipping a trip object because it's missing coordinate data.")
                continue

            # --- Step 1: Extract and calculate all possible values from the fetched trip ---
            start_ts_utc = trip.start_time.astimezone(datetime.timezone.utc)
            distance_km = getattr(trip, 'distance', 0.0) or 0.0
            fuel_consumption_l_100km = getattr(trip, 'average_fuel_consumed', 0.0) or 0.0
            duration_seconds = getattr(trip, 'duration', datetime.timedelta(0)).total_seconds()
            average_speed_kmh = (distance_km / (duration_seconds / 3600)) if duration_seconds > 0 and distance_km > 0 else 0.0
            
            summary = trip._trip.summary if hasattr(trip, '_trip') and hasattr(trip._trip, 'summary') else None
            scores = trip._trip.scores if hasattr(trip, '_trip') and hasattr(trip._trip, 'scores') else None
            hdc = trip._trip.hdc if hasattr(trip, '_trip') and hasattr(trip._trip, 'hdc') else None
            
            # Correct the units for distances (API provides many in meters)
            ev_distance_km = (hdc.ev_distance / 1000) if hdc and hdc.ev_distance is not None else getattr(trip, 'ev_distance', 0.0)
            hdc_charge_dist_km = (hdc.charge_dist / 1000) if hdc and hdc.charge_dist is not None else None
            hdc_eco_dist_km = (hdc.eco_dist / 1000) if hdc and hdc.eco_dist is not None else None
            hdc_power_dist_km = (hdc.power_dist / 1000) if hdc and hdc.power_dist is not None else None
            length_overspeed_km = (summary.length_overspeed / 1000) if summary and summary.length_overspeed is not None else None
            length_highway_km = (summary.length_highway / 1000) if summary and summary.length_highway is not None else None


            route_data = None
            if fetch_full_route and hasattr(trip, 'route') and trip.route:
                route_data = [point.model_dump(mode="json") for point in trip.route]

            KM_TO_MI = 0.621371
            
            new_data = {
                'end_timestamp': trip.end_time.astimezone(datetime.timezone.utc),
                'start_lat': trip.locations.start.lat, 'start_lon': trip.locations.start.lon,
                'end_lat': trip.locations.end.lat, 'end_lon': trip.locations.end.lon,
                'distance_km': distance_km,
                'fuel_consumption_l_100km': fuel_consumption_l_100km,
                'duration_seconds': int(duration_seconds),
                'average_speed_kmh': average_speed_kmh,
                'max_speed_kmh': summary.max_speed if summary else None,
                'countries': summary.countries if summary else None,
                'length_overspeed_km': length_overspeed_km,
                'duration_overspeed_seconds': summary.duration_overspeed if summary else None,
                'length_highway_km': length_highway_km,
                'duration_highway_seconds': summary.duration_highway if summary else None,
                'night_trip': summary.night_trip if summary else None,
                'score_global': scores.global_ if scores else getattr(trip, 'score', None),
                'score_acceleration': scores.acceleration if scores else None,
                'score_braking': scores.braking if scores else None,
                'score_advice': scores.advice if scores else None,
                'score_constant_speed': scores.constant_speed if scores else None,
                'ev_distance_km': ev_distance_km,
                'ev_duration_seconds': hdc.ev_time if hdc and hdc.ev_time is not None else int(getattr(trip, 'ev_duration', datetime.timedelta(0)).total_seconds()),
                'hdc_charge_duration_seconds': hdc.charge_time if hdc else None,
                'hdc_charge_distance_km': hdc_charge_dist_km,
                'hdc_eco_duration_seconds': hdc.eco_time if hdc else None,
                'hdc_eco_distance_km': hdc_eco_dist_km,
                'hdc_power_duration_seconds': hdc.power_time if hdc else None,
                'hdc_power_distance_km': hdc_power_dist_km,
                'distance_mi': distance_km * KM_TO_MI,
                'mpg': (235.214 / fuel_consumption_l_100km) if fuel_consumption_l_100km > 0 else 0.0,
                'mpg_uk': (282.481 / fuel_consumption_l_100km) if fuel_consumption_l_100km > 0 else 0.0,
                'average_speed_mph': average_speed_kmh * KM_TO_MI,
                'ev_distance_mi': (ev_distance_km or 0.0) * KM_TO_MI,
                'route': route_data
            }

            # --- Step 2: Check for existing trip and apply logic ---
            existing_trip = db_session.query(database.Trip).filter_by(vin=vehicle.vin, start_timestamp=start_ts_utc).first()

            if existing_trip:
                # Trip exists. Overwrite with latest data from API, but protect geocoded fields.
                _LOGGER.info(f"Updating trip {existing_trip.id} with new/corrected data from API.")
                for key, value in new_data.items():
                    if key not in PROTECTED_FIELDS:
                        setattr(existing_trip, key, value)
                db_session.commit()
                updated_trips_count += 1
                continue

            # --- Step 3: If no existing trip, create a new one ---
            new_trip = database.Trip(
                vin=vehicle.vin,
                start_timestamp=start_ts_utc,
                start_address="Geocoding...", # Default for new trips
                end_address="Geocoding...",
                **new_data
            )
            db_session.add(new_trip)
            db_session.commit()
            db_session.refresh(new_trip)
            new_trips_count += 1

            # Trigger geocoding in the background
            asyncio.create_task(_reverse_geocode_trip(new_trip.id))

        except Exception as e:
            _LOGGER.warning(f"Could not process a trip summary due to an error: {e}. Skipping.", exc_info=True)
            db_session.rollback()

    _LOGGER.info(f"Trip summary fetch for {vehicle.vin} complete. New: {new_trips_count}, Updated: {updated_trips_count}, Skipped (no changes): {skipped_trips_count}.")
    return {"new": new_trips_count, "updated": updated_trips_count, "skipped": skipped_trips_count}


async def _update_vehicle_statistics(vehicle, vehicle_info_dict):
    """Fetches and processes daily driving statistics for the live dashboard tile."""
    _LOGGER.info(f"Fetching today's statistics for VIN {vehicle.vin}...")

    async def process_stats(stats_obj):
        if not stats_obj: return None
        
        dist = stats_obj.distance or 0.0
        fuel = stats_obj.fuel_consumed or 0.0
        ev_dist = stats_obj.ev_distance or 0.0
        
        non_ev_dist = dist - ev_dist
        distance_for_fuel_calc = non_ev_dist if vehicle_info_dict["is_hybrid"] and non_ev_dist > 0 else dist
        fuel_consumption = (fuel / distance_for_fuel_calc) * 100 if fuel > 0 and distance_for_fuel_calc > 0 else 0.0

        return {
            "distance": dist,
            "fuel_consumed": fuel,
            "calculated_fuel_consumption_l_100km": round(fuel_consumption, 2),
        }

    daily_summary = await vehicle.get_current_day_summary()
    vehicle_info_dict["statistics"]["daily"] = await process_stats(daily_summary)


def _build_vehicle_info_dict(vehicle):
    """Builds the main vehicle information dictionary from the vehicle object."""
    vehicle_info = {
        "vin": vehicle.vin,
        "alias": vehicle.alias or "N/A",
        "is_hybrid": vehicle.type in ["hybrid", "phev"],
        "model_name": getattr(vehicle._vehicle_info, "car_model_name", "Unknown Model"),
        "dashboard": {}, "statistics": {"overall": {}, "daily": {}}, "status": {}
    }

    if vehicle.dashboard:
        d = vehicle.dashboard
        vehicle_info["dashboard"] = {
            "odometer": getattr(d, "odometer", None),
            "fuel_level": getattr(d, "fuel_level", None),
            "total_range": getattr(d, "range", None),
            "fuel_range": getattr(d, "fuel_range", None),
            "battery_level": getattr(d, "battery_level", None),
            "battery_range": getattr(d, "battery_range", None),
            "battery_range_with_ac": getattr(d, "battery_range_with_ac", None),
            "charging_status": getattr(d, "charging_status", None),
            "latitude": getattr(vehicle.location, 'latitude', None) if hasattr(vehicle, 'location') else None,
            "longitude": getattr(vehicle.location, 'longitude', None) if hasattr(vehicle, 'location') else None,
        }

    doors_status = {}
    windows_status = {}
    hood_closed = True
    trunk_closed = True
    trunk_locked = False
    last_update_timestamp = None

    if hasattr(vehicle, 'lock_status') and vehicle.lock_status:
        lock_status = vehicle.lock_status
        
        _LOGGER.debug(f"--- Raw lock_status object for VIN {vehicle.vin} ---")
        _LOGGER.debug(lock_status)
        
        if hasattr(lock_status, 'doors') and lock_status.doors:
            doors = lock_status.doors
            door_map = {
                'driver_seat': 'front_left', 'passenger_seat': 'front_right',
                'driver_rear_seat': 'rear_left', 'passenger_rear_seat': 'rear_right',
            }
            for attr_name, key in door_map.items():
                if hasattr(doors, attr_name):
                    door_obj = getattr(doors, attr_name)
                    _LOGGER.debug(f"Processing door '{key}': raw closed={door_obj.closed}, raw locked={door_obj.locked}")
                    
                    raw_closed = door_obj.closed
                    raw_locked = door_obj.locked
                    locked_status = False if raw_locked is None else raw_locked
                    
                    if raw_closed is not None:
                        closed_status = raw_closed
                    elif locked_status is True:
                        _LOGGER.debug(f"Door '{key}' has closed=None but locked=True. Interpreting as closed.")
                        closed_status = True
                    else:
                        closed_status = False 
                    
                    doors_status[key] = {"closed": closed_status, "locked": locked_status}
                else:
                    doors_status[key] = {"closed": True, "locked": False}
            
            if hasattr(doors, 'trunk'):
                if doors.trunk.closed is not None:
                    trunk_closed = doors.trunk.closed
                if doors.trunk.locked is not None:
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
                    windows_status[key] = {"closed": True if window_obj.closed is None else window_obj.closed}
                else:
                    windows_status[key] = {"closed": True}
        
        if hasattr(lock_status, 'hood') and lock_status.hood.closed is not None:
            hood_closed = lock_status.hood.closed
        if hasattr(lock_status, 'last_update_timestamp') and lock_status.last_update_timestamp:
            # Ensure the datetime object is timezone-aware before formatting
            ts = lock_status.last_update_timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            last_update_timestamp = ts.isoformat()


    vehicle_info["status"] = {
        "doors": doors_status,
        "windows": windows_status,
        "hood_closed": hood_closed,
        "trunk_closed": trunk_closed,
        "trunk_locked": trunk_locked,
        "last_update_timestamp": last_update_timestamp
    }

    notifications_data = []
    if hasattr(vehicle, 'notifications') and vehicle.notifications:
        notifications_data = [
            notification.model_dump(mode="json") for notification in vehicle.notifications
        ]
    vehicle_info["notifications"] = notifications_data

    return vehicle_info

async def _process_vehicle(vehicle, db_session):
    """
    Processes a single vehicle: updates its data, checks odometer, and fetches trips if needed.
    """
    vin = vehicle.vin
    _LOGGER.info(f"Processing vehicle: {vin} ({vehicle.alias})")

    api_retries = settings.get("api_retries", 3)
    api_retry_delay = settings.get("api_retry_delay_seconds", 5)

    for attempt in range(api_retries + 1):
        try:
            await vehicle.update()
            _LOGGER.info(f"Live data updated for VIN: {vin}")
            break
        except ToyotaApiError as e:
            _LOGGER.warning(f"API error during vehicle.update() for VIN {vin} (Attempt {attempt + 1}): {e}")
            if attempt < api_retries:
                await asyncio.sleep(api_retry_delay)
            else:
                _LOGGER.error(f"Failed to update vehicle {vin} after all retries.")
                raise

    vehicle_info = _build_vehicle_info_dict(vehicle)
    await _update_vehicle_statistics(vehicle, vehicle_info)
    
    new_odometer = vehicle_info.get("dashboard", {}).get("odometer")
    if new_odometer is None:
        _LOGGER.warning(f"Odometer data not available for {vin}. Skipping database entry and trip fetch.")
        return vehicle_info

    latest_reading = database.get_latest_reading(vin=vin)
    latest_trip_ts = database.get_latest_trip_timestamp(vin=vin)
    
    is_first_run = not latest_trip_ts
    odometer_changed = not latest_reading or new_odometer > latest_reading.odometer

    if odometer_changed or is_first_run:
        _LOGGER.info(f"New activity detected for {vin}. Odometer: {new_odometer} km. Saving reading and fetching trips.")
        database.add_reading(vehicle_info)
        
        to_date = datetime.date.today()
        from_date = (to_date - datetime.timedelta(days=7)) if is_first_run else latest_trip_ts.date()
        
        _LOGGER.info(f"Auto-fetching recent trips from {from_date} to {to_date}.")
        await _fetch_and_process_trip_summaries(vehicle, db_session, from_date, to_date)
    else:
        _LOGGER.info(f"Odometer for {vin} has not changed. Skipping trip fetch.")
    
    # --- New: Calculate and add overall statistics to the vehicle_info dict ---
    stats = db_session.query(
        func.sum(database.Trip.distance_km).label("total_distance"),
        func.sum(database.Trip.ev_distance_km).label("total_ev_distance"),
        func.sum(database.Trip.fuel_consumption_l_100km * database.Trip.distance_km / 100).label("total_fuel"),
        func.sum(database.Trip.duration_seconds).label("total_duration_seconds")
    ).filter(database.Trip.vin == vin).first()
    
    if stats and stats.total_distance is not None and stats.total_distance > 0:
        total_distance = stats.total_distance
        total_ev_distance = stats.total_ev_distance or 0.0
        total_fuel = stats.total_fuel or 0.0
        
        vehicle_info["statistics"]["overall"] = {
            "total_ev_distance_km": round(total_ev_distance),
            "total_fuel_l": round(total_fuel, 2),
            "total_duration_seconds": stats.total_duration_seconds or 0,
            "ev_ratio_percent": round((total_ev_distance / total_distance) * 100, 1),
            "fuel_consumption_l_100km": round((total_fuel / total_distance) * 100, 2) if total_fuel > 0 else 0.0
        }
        _LOGGER.debug(f"Calculated overall stats for {vin}: {vehicle_info['statistics']['overall']}")
    
    return vehicle_info

async def run_fetch_cycle():
    """
    The main entrypoint for scheduled data fetching.
    """
    _LOGGER.info("Starting scheduled data fetch cycle...")
    username, password = load_credentials()
    if not username or not password:
        _LOGGER.error("Credentials not found. Please set them on the Settings page.")
        return

    client = MyT(username=username, password=password, use_metric=True)
    all_vehicle_data = []
    
    _LOGGER.info("Checking MQTT settings and attempting to initialize client...")
    mqtt_client = mqtt.get_client()
    if not mqtt_client:
        _LOGGER.info("MQTT client not created (check settings or connection errors). Publishing will be skipped.")

    try:
        vin_to_service_history = {}
        if await aiofiles.os.path.exists(CACHE_FILE):
            try:
                async with aiofiles.open(CACHE_FILE, 'r') as f:
                    content = await f.read()
                    existing_cache = json.loads(content)
                for vehicle_data in existing_cache.get("vehicles", []):
                    if "service_history" in vehicle_data and "vin" in vehicle_data:
                        vin_to_service_history[vehicle_data["vin"]] = vehicle_data["service_history"]
                _LOGGER.debug(f"Preserving service history for VINs: {list(vin_to_service_history.keys())}")
            except (IOError, json.JSONDecodeError):
                _LOGGER.warning("Could not read existing cache file to preserve data.")

        await client.login()
        vehicles = await client.get_vehicles()
        if not vehicles:
            _LOGGER.info("No vehicles found for this account.")
            return
            
        db = database.SessionLocal()
        try:
            tasks = [_process_vehicle(v, db) for v in vehicles if v and v.vin]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for res in results:
                if isinstance(res, dict):
                    vin = res.get("vin")
                    if vin in vin_to_service_history:
                        res["service_history"] = vin_to_service_history[vin]
                        _LOGGER.debug(f"Restored service history for VIN {vin}.")
                    all_vehicle_data.append(res)
                    
                    if mqtt_client:
                        mqtt.publish_autodiscovery_configs(mqtt_client, res)
                        _LOGGER.debug(f"Handing over vehicle data for VIN {vin} to MQTT publisher.")
                        mqtt.publish_vehicle_data(mqtt_client, res)

                elif isinstance(res, Exception):
                    _LOGGER.error(f"An error occurred while processing a vehicle: {res}", exc_info=True)
        finally:
            db.close()
        if all_vehicle_data:
            tmp_file = CACHE_FILE.with_suffix(".tmp")
            async with CACHE_LOCK:
                async with aiofiles.open(tmp_file, "w") as f:
                    aware_utcnow = datetime.datetime.now(datetime.timezone.utc)
                    await f.write(json.dumps({"last_updated": aware_utcnow.isoformat(), "vehicles": all_vehicle_data}, indent=2))
                await aiofiles.os.replace(tmp_file, CACHE_FILE)
            _LOGGER.info(f"Successfully fetched and cached data for {len(all_vehicle_data)} vehicle(s).")
        else:
            _LOGGER.info("No new vehicle data was processed, cache file not updated.")
            
    except Exception as e:
        _LOGGER.error(f"An unexpected error occurred in the fetch cycle: {e}", exc_info=True)
    finally:
        if mqtt_client:
            _LOGGER.debug("Disconnecting MQTT client.")
            mqtt.disconnect(mqtt_client)
            
        if client and hasattr(client, "_session") and client._session and not client._session.is_closed:
            await client._session.aclose()
            _LOGGER.info("Pytoyoda client session closed.")


async def backfill_trips(vin: str, period: str):
    """Manually fetches historical trips for a specific vehicle and period."""
    _LOGGER.info(f"Starting manual trip backfill for VIN {vin}, period: {period}")
    username, password = load_credentials()
    if not username or not password:
        return {"error": "Credentials not found."}

    client = MyT(username=username, password=password, use_metric=True)
    try:
        await client.login()
        target_vehicle = next((v for v in await client.get_vehicles() if v.vin == vin), None)
        if not target_vehicle:
            return {"error": f"Vehicle with VIN {vin} not found on this account."}

        to_date = datetime.date.today()
        period_map = {
            "week": 7, "month": 31, "year": 365, "all": 365 * 5
        }
        if period not in period_map:
            return {"error": "Invalid period specified."}
        from_date = to_date - datetime.timedelta(days=period_map[period])

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
            
async def backfill_geocoding():
    """Finds all trips that haven't been geocoded and queues them for processing."""
    _LOGGER.info("Starting manual geocoding backfill process...")
    db = database.SessionLocal()
    try:
        pending_trips = db.query(database.Trip).filter(database.Trip.start_address == "Geocoding...").all()
        if not pending_trips:
            return {"message": "No trips require geocoding."}

        _LOGGER.info(f"Found {len(pending_trips)} trips to geocode. Queueing tasks...")
        for trip in pending_trips:
            asyncio.create_task(_reverse_geocode_trip(trip.id))
        
        return {"message": f"Successfully queued {len(pending_trips)} trips for geocoding."}
    finally:
        db.close()

async def fetch_service_history(vin: str):
    """Fetches the full service history for a given vehicle."""
    _LOGGER.info(f"Fetching service history for VIN {vin}...")
    username, password = load_credentials()
    if not username or not password:
        return {"error": "Credentials not found."}

    client = MyT(username=username, password=password, use_metric=True)
    try:
        await client.login()
        history_response = await client._api.get_service_history(vin=vin)
        
        if history_response and history_response.payload:
            return history_response.payload.model_dump(mode="json")
        else:
            return {"service_histories": []}
            
    except Exception as e:
        _LOGGER.error(f"Error fetching service history for VIN {vin}: {e}", exc_info=True)
        return {"error": "An error occurred during the service history fetch."}
    finally:
        if client and hasattr(client, "_session") and client._session and not client._session.is_closed:
            await client._session.aclose()