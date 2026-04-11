from abc import ABC, abstractmethod
from typing import Optional
import httpx
import logging

_LOGGER = logging.getLogger(__name__)


class BaseGeocoder(ABC):
    """Abstract base class for all geocoding providers."""

    @abstractmethod
    async def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        """Performs reverse geocoding to return a human-readable address."""
        pass


class NominatimGeocoder(BaseGeocoder):
    """Implementation using OpenStreetMap's Nominatim service."""

    async def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        if not lat or not lon:
            return None

        headers = {"User-Agent": "MyToyota-Dashboard/1.0"}
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&addressdetails=1"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                return self._format_address(data)
        except Exception as e:
            _LOGGER.error(f"Nominatim error: {e}")
            return "Unavailable"

    def _format_address(self, data: dict) -> str:
        address = data.get("address", {})
        road = address.get("road")
        house_number = address.get("house_number")
        city = address.get("city") or address.get("town") or address.get("village")
        postcode = address.get("postcode")

        parts = []
        if road:
            parts.append(f"{road} {house_number}" if house_number else road)
        if postcode:
            parts.append(postcode)
        if city:
            parts.append(city)

        return ", ".join(parts) if parts else data.get("display_name", "Unavailable")


class OpenCageGeocoder(BaseGeocoder):
    """Implementation using OpenCage API."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        if not self.api_key:
            _LOGGER.error("OpenCage API key is missing.")
            return None

        url = f"https://api.opencagedata.com/geocode/v1/json?q={lat}+{lon}&key={self.api_key}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data.get("results"):
                    return data["results"][0].get("formatted")
                return "Unavailable"
        except Exception as e:
            _LOGGER.error(f"OpenCage error: {e}")
            return "Unavailable"


class GoogleMapsGeocoder(BaseGeocoder):
    """Implementation using Google Maps API."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        if not self.api_key:
            _LOGGER.error("Google Maps API key is extremely critical.")
            return None

        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={self.api_key}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data.get("results"):
                    return data["results"][0].get("formatted_address")
                return "Unavailable"
        except Exception as e:
            _LOGGER.error(f"Google Maps error: {e}")
            return "Unavailable"


class GeocoderFactory:
    """Factory to instantiate the configured geocoder."""

    @staticmethod
    def get_geocoder(config_manager) -> BaseGeocoder:
        try:
            geocoding_cfg = config_manager.settings.get("geocoding", {})
        except Exception:
            geocoding_cfg = {}

        provider = geocoding_cfg.get("provider", "nominatim").lower()

        if provider == "opencage":
            return OpenCageGeocoder(geocoding_cfg.get("opencage_api_key", ""))
        elif provider == "google_maps":
            return GoogleMapsGeocoder(geocoding_cfg.get("google_maps_api_key", ""))
        else:
            return NominatimGeocoder()
