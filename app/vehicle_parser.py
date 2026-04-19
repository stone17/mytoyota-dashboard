# app/vehicle_parser.py
import datetime
import logging

_LOGGER = logging.getLogger(__name__)

class VehicleParser:
    """Extracts and formats live vehicle status, dashboard, and daily statistics from Pytoyoda models."""

    def __init__(self, vehicle, reverse_geocode_enabled=False, geocode_callback=None):
        self.vehicle = vehicle
        self.reverse_geocode_enabled = reverse_geocode_enabled
        self.geocode_callback = geocode_callback

    async def build_info_dict(self):
        """Builds the main vehicle information dictionary."""
        aware_utcnow = datetime.datetime.now(datetime.timezone.utc)

        vehicle_info = {
            "vin": self.vehicle.vin,
            "alias": self.vehicle.alias or "N/A",
            "is_hybrid": self.vehicle.type in ["hybrid", "phev"],
            "model_name": getattr(self.vehicle._vehicle_info, "car_model_name", "Unknown Model"),
            "dashboard": {},
            "statistics": {"overall": {}, "daily": {}},
            "status": {},
            "notifications": [],
            "last_updated": aware_utcnow,
        }

        if self.vehicle.dashboard:
            d = self.vehicle.dashboard
            latitude = getattr(self.vehicle.location, "latitude", None) if hasattr(self.vehicle, "location") else None
            longitude = getattr(self.vehicle.location, "longitude", None) if hasattr(self.vehicle, "location") else None

            address = None
            if latitude and longitude and self.reverse_geocode_enabled and self.geocode_callback:
                address = await self.geocode_callback(latitude, longitude)

            vehicle_info["dashboard"] = {
                "odometer": getattr(d, "odometer", None),
                "fuel_level": getattr(d, "fuel_level", None),
                "total_range": getattr(d, "range", None),
                "fuel_range": getattr(d, "fuel_range", None),
                "battery_level": getattr(d, "battery_level", None),
                "battery_range": getattr(d, "battery_range", None),
                "battery_range_with_ac": getattr(d, "battery_range_with_ac", None),
                "charging_status": getattr(d, "charging_status", None),
                "latitude": latitude,
                "longitude": longitude,
                "address": address,
            }

        self._parse_lock_status(vehicle_info, aware_utcnow)

        if hasattr(self.vehicle, "notifications") and self.vehicle.notifications:
            vehicle_info["notifications"] = [
                notification.model_dump(mode="json") for notification in self.vehicle.notifications
            ]

        return vehicle_info

    def _parse_lock_status(self, vehicle_info, aware_utcnow):
        """Handles the complex nesting and fallback logic for vehicle doors and windows."""
        doors_status = {
            "front_left": {"closed": True, "locked": False},
            "front_right": {"closed": True, "locked": False},
            "rear_left": {"closed": True, "locked": False},
            "rear_right": {"closed": True, "locked": False},
        }
        windows_status = {
            "front_left": {"closed": True}, "front_right": {"closed": True},
            "rear_left": {"closed": True}, "rear_right": {"closed": True},
        }
        hood_closed, trunk_closed, trunk_locked = True, True, False
        last_update_timestamp = aware_utcnow.isoformat()
        lock_status_error = False

        if hasattr(self.vehicle, "lock_status") and self.vehicle.lock_status:
            try:
                lock_status = self.vehicle.lock_status
                ts = getattr(lock_status, "last_update_timestamp", getattr(lock_status, "timestamp", None))
                if ts:
                    if isinstance(ts, datetime.datetime):
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=datetime.timezone.utc)
                        last_update_timestamp = ts.isoformat()
                    else:
                        last_update_timestamp = str(ts)

                if hasattr(lock_status, "doors") and lock_status.doors:
                    door_map = {
                        "driver_seat": "front_left", "passenger_seat": "front_right",
                        "driver_rear_seat": "rear_left", "passenger_rear_seat": "rear_right",
                    }
                    for attr_name, key in door_map.items():
                        if hasattr(lock_status.doors, attr_name) and getattr(lock_status.doors, attr_name) is not None:
                            door_obj = getattr(lock_status.doors, attr_name)
                            raw_locked = door_obj.locked
                            locked_status = False if raw_locked is None else raw_locked
                            closed_status = door_obj.closed if door_obj.closed is not None else locked_status
                            doors_status[key] = {"closed": closed_status, "locked": locked_status}

                    if hasattr(lock_status.doors, "trunk") and lock_status.doors.trunk is not None:
                        if lock_status.doors.trunk.closed is not None:
                            trunk_closed = lock_status.doors.trunk.closed
                        if lock_status.doors.trunk.locked is not None:
                            trunk_locked = lock_status.doors.trunk.locked

                if hasattr(lock_status, "windows") and lock_status.windows:
                    window_map = {
                        "driver_seat": "front_left", "passenger_seat": "front_right",
                        "driver_rear_seat": "rear_left", "passenger_rear_seat": "rear_right",
                    }
                    for attr_name, key in window_map.items():
                        if hasattr(lock_status.windows, attr_name) and getattr(lock_status.windows, attr_name) is not None:
                            window_obj = getattr(lock_status.windows, attr_name)
                            windows_status[key] = {"closed": True if window_obj.closed is None else window_obj.closed}

                if hasattr(lock_status, "hood") and lock_status.hood is not None and lock_status.hood.closed is not None:
                    hood_closed = lock_status.hood.closed

            except Exception as e:
                _LOGGER.error(f"Error parsing lock status for VIN {self.vehicle.vin}: {e}", exc_info=True)
                lock_status_error = True

        vehicle_info["status"] = {
            "doors": doors_status, "windows": windows_status,
            "hood_closed": hood_closed, "trunk_closed": trunk_closed,
            "trunk_locked": trunk_locked, "last_update_timestamp": last_update_timestamp,
            "error": lock_status_error,
        }

    async def update_daily_statistics(self, vehicle_info_dict):
        """Fetches and calculates today's daily statistics."""
        _LOGGER.info(f"Fetching today's statistics for VIN {self.vehicle.vin}...")

        daily_summary = await self.vehicle.get_current_day_summary()
        if not daily_summary:
            return

        def safe_get(obj, attr, default=0.0):
            try:
                val = getattr(obj, attr)
                return val if val is not None else default
            except (AttributeError, TypeError):
                return default

        dist = safe_get(daily_summary, "distance")
        fuel = safe_get(daily_summary, "fuel_consumed")
        ev_dist = safe_get(daily_summary, "ev_distance")

        non_ev_dist = dist - ev_dist
        distance_for_fuel_calc = non_ev_dist if vehicle_info_dict["is_hybrid"] and non_ev_dist > 0 else dist
        fuel_consumption = ((fuel / distance_for_fuel_calc) * 100) if fuel > 0 and distance_for_fuel_calc > 0 else 0.0

        vehicle_info_dict["statistics"]["daily"] = {
            "distance": dist,
            "fuel_consumed": fuel,
            "calculated_fuel_consumption_l_100km": round(fuel_consumption, 2),
        }