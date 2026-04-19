# app/trip_analyzer.py
import asyncio
import datetime
import logging
from pytoyoda.models.trips import Trip as PytoyodaTrip
from . import database
from .config import config_manager

_LOGGER = logging.getLogger(__name__)

class TripCoordinates:
    """Robustly extracts trip coordinates from varying Pytoyoda model structures."""
    def __init__(self, trip_obj):
        self.start_lat = getattr(trip_obj, "start_lat", None)
        self.start_lon = getattr(trip_obj, "start_lon", None)
        self.end_lat = getattr(trip_obj, "end_lat", None)
        self.end_lon = getattr(trip_obj, "end_lon", None)

        if self.start_lat is None and hasattr(trip_obj, "locations") and trip_obj.locations:
            if hasattr(trip_obj.locations, "start") and trip_obj.locations.start:
                self.start_lat = getattr(trip_obj.locations.start, "lat", None)
                self.start_lon = getattr(trip_obj.locations.start, "lon", None)
            if hasattr(trip_obj.locations, "end") and trip_obj.locations.end:
                self.end_lat = getattr(trip_obj.locations.end, "lat", None)
                self.end_lon = getattr(trip_obj.locations.end, "lon", None)
        
        if self.start_lat is None and hasattr(trip_obj, "route") and trip_obj.route:
            start_node = trip_obj.route[0]
            end_node = trip_obj.route[-1]
            
            self.start_lat = start_node.get("lat") if isinstance(start_node, dict) else getattr(start_node, "lat", None)
            self.start_lon = start_node.get("lon") if isinstance(start_node, dict) else getattr(start_node, "lon", None)
            self.end_lat = end_node.get("lat") if isinstance(end_node, dict) else getattr(end_node, "lat", None)
            self.end_lon = end_node.get("lon") if isinstance(end_node, dict) else getattr(end_node, "lon", None)
    
    @property
    def is_valid(self):
        return self.start_lat is not None and self.start_lon is not None


class TripAnalyzer:
    """Handles fetching, parsing, and storing vehicle trip data."""
    
    PROTECTED_FIELDS = {"start_address", "end_address", "countries"}
    KM_TO_MI = 0.621371

    def __init__(self, vehicle, db_session, geocode_callback=None):
        self.vehicle = vehicle
        self.db_session = db_session
        self.geocode_callback = geocode_callback

    async def fetch_and_process(self, from_date, to_date):
        _LOGGER.info(f"Fetching trip summaries for VIN {self.vehicle.vin} from {from_date} to {to_date}...")

        fetch_full_route = config_manager.settings.get("fetch_full_trip_route", False)
        all_trips = await self.vehicle.get_trips(
            from_date=from_date, to_date=to_date, full_route=fetch_full_route
        )

        if not isinstance(all_trips, list):
            _LOGGER.error(f"Expected a list of trips, but got {type(all_trips)}. Aborting trip fetch.")
            return {"new": 0, "updated": 0, "skipped": 0, "error": "Invalid response from API library"}

        _LOGGER.info(f"API returned a total of {len(all_trips)} trips for the period.")
        
        counts = {"new": 0, "updated": 0, "skipped": 0}

        for trip in all_trips:
            try:
                coords = TripCoordinates(trip)
                if not coords.is_valid:
                    _LOGGER.warning("Skipping a trip object because it's missing coordinate data.")
                    continue

                new_data = self._extract_trip_data(trip, coords, fetch_full_route)
                self._upsert_trip(new_data, counts)

            except Exception as e:
                _LOGGER.warning(f"Could not process a trip summary due to an error: {e}. Skipping.", exc_info=True)
                self.db_session.rollback()

        _LOGGER.info(f"Trip summary fetch complete. New: {counts['new']}, Updated: {counts['updated']}, Skipped: {counts['skipped']}.")
        return counts

    def _extract_trip_data(self, trip, coords, fetch_full_route):
        """Extracts and normalizes data points from the API trip object."""
        distance_km = getattr(trip, "distance", 0.0) or 0.0
        fuel_consumption_l_100km = getattr(trip, "average_fuel_consumed", 0.0) or 0.0
        duration_seconds = getattr(trip, "duration", datetime.timedelta(0)).total_seconds()
        average_speed_kmh = (distance_km / (duration_seconds / 3600)) if duration_seconds > 0 and distance_km > 0 else 0.0

        summary = trip._trip.summary if hasattr(trip, "_trip") and hasattr(trip._trip, "summary") else None
        scores = trip._trip.scores if hasattr(trip, "_trip") and hasattr(trip._trip, "scores") else None
        hdc = trip._trip.hdc if hasattr(trip, "_trip") and hasattr(trip._trip, "hdc") else None

        ev_distance_km = (hdc.ev_distance / 1000) if hdc and hdc.ev_distance is not None else getattr(trip, "ev_distance", 0.0)
        
        route_data = None
        if fetch_full_route and hasattr(trip, "route") and trip.route:
            route_data = [point.model_dump(mode="json") for point in trip.route]

        return {
            "start_timestamp": trip.start_time.astimezone(datetime.timezone.utc),
            "end_timestamp": trip.end_time.astimezone(datetime.timezone.utc),
            "start_lat": coords.start_lat,
            "start_lon": coords.start_lon,
            "end_lat": coords.end_lat,
            "end_lon": coords.end_lon,
            "distance_km": distance_km,
            "fuel_consumption_l_100km": fuel_consumption_l_100km,
            "duration_seconds": int(duration_seconds),
            "average_speed_kmh": average_speed_kmh,
            "max_speed_kmh": summary.max_speed if summary else None,
            "countries": summary.countries if summary else None,
            "length_overspeed_km": (summary.length_overspeed / 1000) if summary and summary.length_overspeed is not None else None,
            "duration_overspeed_seconds": summary.duration_overspeed if summary else None,
            "length_highway_km": (summary.length_highway / 1000) if summary and summary.length_highway is not None else None,
            "duration_highway_seconds": summary.duration_highway if summary else None,
            "night_trip": summary.night_trip if summary else None,
            "score_global": scores.global_ if scores else getattr(trip, "score", None),
            "score_acceleration": scores.acceleration if scores else None,
            "score_braking": scores.braking if scores else None,
            "score_advice": scores.advice if scores else None,
            "score_constant_speed": scores.constant_speed if scores else None,
            "ev_distance_km": ev_distance_km,
            "ev_duration_seconds": hdc.ev_time if hdc and hdc.ev_time is not None else int(getattr(trip, "ev_duration", datetime.timedelta(0)).total_seconds()),
            "hdc_charge_duration_seconds": hdc.charge_time if hdc else None,
            "hdc_charge_distance_km": (hdc.charge_dist / 1000) if hdc and hdc.charge_dist is not None else None,
            "hdc_eco_duration_seconds": hdc.eco_time if hdc else None,
            "hdc_eco_distance_km": (hdc.eco_dist / 1000) if hdc and hdc.eco_dist is not None else None,
            "hdc_power_duration_seconds": hdc.power_time if hdc else None,
            "hdc_power_distance_km": (hdc.power_dist / 1000) if hdc and hdc.power_dist is not None else None,
            "distance_mi": distance_km * self.KM_TO_MI,
            "mpg": (235.214 / fuel_consumption_l_100km) if fuel_consumption_l_100km > 0 else 0.0,
            "mpg_uk": (282.481 / fuel_consumption_l_100km) if fuel_consumption_l_100km > 0 else 0.0,
            "average_speed_mph": average_speed_kmh * self.KM_TO_MI,
            "ev_distance_mi": (ev_distance_km or 0.0) * self.KM_TO_MI,
            "route": route_data,
        }

    def _upsert_trip(self, new_data, counts):
        """Inserts or updates the trip in the database and triggers geocoding."""
        existing_trip = self.db_session.query(database.Trip).filter_by(
            vin=self.vehicle.vin, start_timestamp=new_data["start_timestamp"]
        ).first()

        start_ts_utc = new_data.pop("start_timestamp")

        if existing_trip:
            for key, value in new_data.items():
                if key not in self.PROTECTED_FIELDS:
                    setattr(existing_trip, key, value)
            self.db_session.commit()
            counts["updated"] += 1
            if not existing_trip.countries and self.geocode_callback:
                asyncio.create_task(self.geocode_callback(existing_trip.id))
        else:
            new_trip = database.Trip(
                vin=self.vehicle.vin,
                start_timestamp=start_ts_utc,
                start_address="Geocoding...",
                end_address="Geocoding...",
                **new_data,
            )
            self.db_session.add(new_trip)
            self.db_session.commit()
            self.db_session.refresh(new_trip)
            counts["new"] += 1
            if self.geocode_callback:
                asyncio.create_task(self.geocode_callback(new_trip.id))