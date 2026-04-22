# app/fetcher.py
import asyncio
import json
import datetime
import logging
from typing import Optional

import aiofiles
import aiofiles.os
from pytoyoda.client import MyT
from pytoyoda.exceptions import ToyotaApiError
from sqlalchemy import func

from . import database
from .credentials_manager import load_credentials
from .config import config_manager, DATA_DIR
from .geocoder import GeocoderFactory
from .toyota_interceptor import PatchedController
from .trip_analyzer import TripAnalyzer
from .vehicle_parser import VehicleParser

_LOGGER = logging.getLogger(__name__)

CACHE_FILE = DATA_DIR / "vehicle_data.json"
CACHE_LOCK = asyncio.Lock()
GEOCODE_SEMAPHORE = asyncio.Semaphore(1)


async def _reverse_geocode_trip(trip_id: int, force: bool = False):
    """Performs reverse geocoding for a specific trip, respecting the semaphore."""
    async with GEOCODE_SEMAPHORE:
        _LOGGER.info(f"Starting geocoding for trip {trip_id}...")
        db = database.SessionLocal()
        try:
            trip = db.query(database.Trip).filter(database.Trip.id == trip_id).first()
            if not trip:
                return
                
            needs_address = force or trip.start_address == "Geocoding..."
            needs_countries = force or not trip.countries

            if not needs_address and not needs_countries:
                return

            if not config_manager.settings.get("reverse_geocode_enabled", True):
                if needs_address:
                    trip.start_address = f"{trip.start_lat}, {trip.start_lon}"
                    trip.end_address = f"{trip.end_lat}, {trip.end_lon}"
                db.commit()
                return

            geocoder = GeocoderFactory.get_geocoder(config_manager)
            countries = set()
            
            if trip.start_lat is not None and trip.start_lon is not None:
                start_addr, start_country = await geocoder.reverse_geocode(trip.start_lat, trip.start_lon)
                if needs_address:
                    trip.start_address = start_addr or "Unknown"
                if needs_countries and start_country:
                    countries.add(start_country)

            if trip.end_lat is not None and trip.end_lon is not None:
                end_addr, end_country = await geocoder.reverse_geocode(trip.end_lat, trip.end_lon)
                if needs_address:
                    trip.end_address = end_addr or "Unknown"
                if needs_countries and end_country:
                    countries.add(end_country)

            if needs_countries and countries:
                trip.countries = sorted(countries)

            db.commit()
            _LOGGER.info(f"Successfully geocoded trip {trip_id}.")
        except Exception as e:
            _LOGGER.error(f"Error during background geocoding for trip {trip_id}: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()


async def _process_vehicle(vehicle):
    """Processes a single vehicle: updates its data, checks odometer, and fetches trips if needed."""
    vin = vehicle.vin
    _LOGGER.info(f"Processing vehicle: {vin} ({vehicle.alias})")

    api_retries = config_manager.settings.get("api_retries", 3)
    api_retry_delay = config_manager.settings.get("api_retry_delay_seconds", 5)

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

    # 1. Parse Vehicle Info
    reverse_geocode_enabled = config_manager.settings.get("reverse_geocode_enabled", False)
    geocoder = None

    async def dashboard_geocode_wrapper(lat, lon):
        nonlocal geocoder
        if not reverse_geocode_enabled:
            return None
        if geocoder is None:
            geocoder = GeocoderFactory.get_geocoder(config_manager)
        address, _ = await geocoder.reverse_geocode(lat, lon)
        if address == "Unavailable":
            return None
        return address

    parser = VehicleParser(vehicle, reverse_geocode_enabled, geocode_callback=dashboard_geocode_wrapper)

    vehicle_info = await parser.build_info_dict()
    try:
        await parser.update_daily_statistics(vehicle_info)
    except Exception as e:
        _LOGGER.error(f"Failed to fetch daily statistics for VIN {vin}: {e}", exc_info=True)

    # Initialize a dedicated DB session for this vehicle's task
    db_session = database.SessionLocal()
    try:
        # 2. Check Odometer & Fetch Trips
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
            try:
                analyzer = TripAnalyzer(vehicle, db_session, geocode_callback=_reverse_geocode_trip)
                await analyzer.fetch_and_process(from_date, to_date)
            except Exception as e:
                _LOGGER.error(f"Failed to auto-fetch trips for VIN {vin}: {e}", exc_info=True)
        else:
            _LOGGER.info(f"Odometer for {vin} has not changed. Skipping trip fetch.")

        # 3. Calculate Overall Database Statistics
        vehicle_info["statistics"]["overall"] = {
            "total_ev_distance_km": 0, "total_fuel_l": 0.0, "total_duration_seconds": 0,
            "ev_ratio_percent": 0.0, "fuel_consumption_l_100km": 0.0, "total_highway_distance_km": 0, "score_global": None,
        }

        stats = (
            db_session.query(
                func.sum(database.Trip.distance_km).label("total_distance"),
                func.sum(database.Trip.ev_distance_km).label("total_ev_distance"),
                func.sum(database.Trip.fuel_consumption_l_100km * database.Trip.distance_km / 100).label("total_fuel"),
                func.sum(database.Trip.duration_seconds).label("total_duration_seconds"),
                func.sum(database.Trip.length_highway_km).label("total_highway_distance"),
                func.avg(database.Trip.score_global).label("score_global"),
            )
            .filter(database.Trip.vin == vin)
            .first()
        )

        if stats and stats.total_distance is not None and stats.total_distance > 0:
            total_distance = stats.total_distance
            total_ev_distance = stats.total_ev_distance or 0.0
            total_fuel = stats.total_fuel or 0.0
            total_highway_distance = stats.total_highway_distance or 0.0

            vehicle_info["statistics"]["overall"].update({
                "total_ev_distance_km": round(total_ev_distance),
                "total_fuel_l": round(total_fuel, 2),
                "total_duration_seconds": stats.total_duration_seconds or 0,
                "ev_ratio_percent": round((total_ev_distance / total_distance) * 100, 1),
                "fuel_consumption_l_100km": round((total_fuel / total_distance) * 100, 2) if total_fuel > 0 else 0.0,
                "total_highway_distance_km": round(total_highway_distance),
                "score_global": round(stats.score_global) if stats.score_global is not None else None,
            })

        return vehicle_info
    finally:
        # Safely close the task-specific session
        db_session.close()


async def run_fetch_cycle():
    """The main entrypoint for scheduled data fetching."""
    _LOGGER.info("Starting scheduled data fetch cycle...")
    username, password = load_credentials()
    if not username or not password:
        _LOGGER.error("Credentials not found. Please set them on the Settings page.")
        return

    client = MyT(username=username, password=password, use_metric=True, controller_class=PatchedController)
    all_vehicle_data = []

    try:
        vin_to_service_history = {}
        if await aiofiles.os.path.exists(CACHE_FILE):
            try:
                async with aiofiles.open(CACHE_FILE, "r") as f:
                    content = await f.read()
                    existing_cache = json.loads(content)
                for vehicle_data in existing_cache.get("vehicles", []):
                    if "service_history" in vehicle_data and "vin" in vehicle_data:
                        vin_to_service_history[vehicle_data["vin"]] = vehicle_data["service_history"]
            except (IOError, json.JSONDecodeError):
                _LOGGER.warning("Could not read existing cache file to preserve data.")

        await client.login()
        vehicles = await client.get_vehicles()
        if not vehicles:
            _LOGGER.info("No vehicles found for this account.")
            return

        # Fire concurrent processing tasks, each manages its own DB session internally
        tasks = [_process_vehicle(v) for v in vehicles if v and v.vin]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, dict):
                vin = res.get("vin")
                if vin in vin_to_service_history:
                    res["service_history"] = vin_to_service_history[vin]
                all_vehicle_data.append(res)
            elif isinstance(res, Exception):
                _LOGGER.error(f"An error occurred while processing a vehicle: {res}", exc_info=res)

        if all_vehicle_data:
            tmp_file = CACHE_FILE.with_suffix(".tmp")
            async with CACHE_LOCK:
                async with aiofiles.open(tmp_file, "w") as f:
                    aware_utcnow = datetime.datetime.now(datetime.timezone.utc)
                    await f.write(json.dumps({
                        "last_updated": aware_utcnow.isoformat(),
                        "vehicles": all_vehicle_data,
                    }, indent=2, default=str))
                await aiofiles.os.replace(tmp_file, CACHE_FILE)
            _LOGGER.info(f"Successfully fetched and cached data for {len(all_vehicle_data)} vehicle(s).")

    except Exception as e:
        _LOGGER.error(f"An unexpected error occurred in the fetch cycle: {e}", exc_info=True)
        return None
    finally:
        if hasattr(client, "_session") and client._session and not client._session.is_closed:
            await client._session.aclose()

    return all_vehicle_data


async def backfill_trips(vin: str, period: str):
    """Manually fetches historical trips for a specific vehicle and period."""
    _LOGGER.info(f"Starting manual trip backfill for VIN {vin}, period: {period}")
    username, password = load_credentials()
    if not username or not password:
        return {"error": "Credentials not found."}

    client = MyT(username=username, password=password, use_metric=True, controller_class=PatchedController)
    try:
        await client.login()
        target_vehicle = next((v for v in await client.get_vehicles() if v.vin == vin), None)
        if not target_vehicle:
            return {"error": f"Vehicle with VIN {vin} not found on this account."}

        to_date = datetime.date.today()
        period_map = {"week": 7, "month": 31, "year": 365, "all": 365 * 5}
        if period not in period_map:
            return {"error": "Invalid period specified."}
        from_date = to_date - datetime.timedelta(days=period_map[period])

        db = database.SessionLocal()
        try:
            analyzer = TripAnalyzer(target_vehicle, db, geocode_callback=_reverse_geocode_trip)
            result = await analyzer.fetch_and_process(from_date, to_date)
            return {"message": f"Fetch for '{period}' complete.", **result}
        finally:
            db.close()
    except Exception as e:
        _LOGGER.error(f"Error during trip backfill: {e}", exc_info=True)
        return {"error": "An internal error occurred during the fetch."}
    finally:
        if hasattr(client, "_session") and client._session and not client._session.is_closed:
            await client._session.aclose()


async def backfill_geocoding(force_all: bool = False):
    """Finds trips to geocode and queues them for processing."""
    _LOGGER.info(f"Starting manual geocoding backfill process (force_all={force_all})...")
    db = database.SessionLocal()
    try:
        query = db.query(database.Trip)
        if not force_all:
            query = query.filter(database.Trip.start_address == "Geocoding...")

        pending_trips = query.all()
        if not pending_trips:
            return {"message": "No trips require geocoding."}

        for trip in pending_trips:
            asyncio.create_task(_reverse_geocode_trip(trip.id, force=force_all))

        return {"message": f"Successfully queued {len(pending_trips)} trips for geocoding."}
    finally:
        db.close()


async def fetch_service_history(vin: str):
    """Fetches the full service history for a given vehicle."""
    _LOGGER.info(f"Fetching service history for VIN {vin}...")
    username, password = load_credentials()
    if not username or not password:
        return {"error": "Credentials not found."}

    client = MyT(username=username, password=password, use_metric=True, controller_class=PatchedController)
    try:
        await client.login()
        history_response = await client._api.get_service_history(vin=vin)

        if history_response and history_response.payload:
            return history_response.payload.model_dump(mode="json")
        return {"service_histories": []}
    except Exception as e:
        _LOGGER.error(f"Error fetching service history for VIN {vin}: {e}", exc_info=True)
        return {"error": "An error occurred during the service history fetch."}
    finally:
        if hasattr(client, "_session") and client._session and not client._session.is_closed:
            await client._session.aclose()