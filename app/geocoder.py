from abc import ABC, abstractmethod
from typing import Optional
import httpx
import logging
import asyncio

_LOGGER = logging.getLogger(__name__)


class BaseGeocoder(ABC):
    """Abstract base class for all geocoding providers."""

    @abstractmethod
    async def reverse_geocode(self, lat: float, lon: float) -> tuple[Optional[str], Optional[str]]:
        """Performs reverse geocoding to return a tuple of (human-readable address, country_code)."""
        pass


class NominatimGeocoder(BaseGeocoder):
    """Implementation using OpenStreetMap's Nominatim service."""

    async def reverse_geocode(self, lat: float, lon: float) -> tuple[Optional[str], Optional[str]]:
        if not lat or not lon:
            return None, None

        # Centralized rate limit: Guarantee a safe buffer before EVERY request
        await asyncio.sleep(1.5) 

        headers = {"User-Agent": "Vehicle-Dashboard/1.0"}
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&addressdetails=1"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                address = data.get("address", {})
                country_code = address.get("country_code", "").upper() or None
                
                return self._format_address(data), country_code
        except Exception as e:
            _LOGGER.error(f"Nominatim error: {e}")
            return "Unavailable", None

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

    async def reverse_geocode(self, lat: float, lon: float) -> tuple[Optional[str], Optional[str]]:
        if not self.api_key:
            _LOGGER.error("OpenCage API key is missing.")
            return None, None

        # Rate limit: OpenCage free tier allows a maximum of 1 request per second
        await asyncio.sleep(1.1)

        url = f"https://api.opencagedata.com/geocode/v1/json?q={lat}+{lon}&key={self.api_key}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data.get("results"):
                    res = data["results"][0]
                    country_code = res.get("components", {}).get("country_code", "").upper() or None
                    return res.get("formatted"), country_code
                return "Unavailable", None
        except Exception as e:
            _LOGGER.error(f"OpenCage error: {e}")
            return "Unavailable", None


class GoogleMapsGeocoder(BaseGeocoder):
    """Implementation using Google Maps API."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def reverse_geocode(self, lat: float, lon: float) -> tuple[Optional[str], Optional[str]]:
        if not self.api_key:
            _LOGGER.error("Google Maps API key is extremely critical.")
            return None, None

        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={self.api_key}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data.get("results"):
                    res = data["results"][0]
                    formatted_address = res.get("formatted_address")
                    
                    country_code = None
                    for component in res.get("address_components", []):
                        if "country" in component.get("types", []):
                            country_code = component.get("short_name", "").upper() or None
                            break
                            
                    return formatted_address, country_code
                return "Unavailable", None
        except Exception as e:
            _LOGGER.error(f"Google Maps error: {e}")
            return "Unavailable", None


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
