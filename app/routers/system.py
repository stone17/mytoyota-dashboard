# app/routers/system.py
import logging
import subprocess
import yaml
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Body
from pydantic import BaseModel

from .. import fetcher
from .. import database
from ..credentials_manager import get_username, save_credentials
from ..config import config_manager
from ..logging_config import setup_logging
from .vehicles import get_cached_vehicle_data

_LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["system"])

@router.get("/geocode_status")
def get_geocode_status():
    """API endpoint to get the number of trips pending geocoding."""
    db = database.SessionLocal()
    try:
        pending_count = (
            db.query(database.Trip)
            .filter(database.Trip.start_address == "Geocoding...")
            .count()
        )
        total_count = db.query(database.Trip).count()
        return {"pending": pending_count, "total": total_count}
    finally:
        db.close()

@router.post("/update")
async def update_application():
    """
    Updates the application by pulling the latest changes from the git repository
    and restarting the docker-compose service.
    """
    try:
        logging.info("Update process started via API.")

        # Sanity Check 1: Check for unmerged changes
        logging.info("Checking for unmerged changes...")
        status_process = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True
        )
        if status_process.stdout:
            logging.error(f"Unmerged changes detected:\n{status_process.stdout}")
            raise HTTPException(
                status_code=400,
                detail="There are unmerged changes in the repository. Please resolve them before updating.",
            )

        # Step 1: Git Pull
        logging.info("Pulling latest changes from git...")
        pull_process = subprocess.run(["git", "pull"], capture_output=True, text=True)
        if pull_process.returncode != 0:
            logging.error(f"Git pull failed: {pull_process.stderr}")
            raise HTTPException(
                status_code=500, detail=f"Git pull failed: {pull_process.stderr}"
            )

        update_message = "No new updates."
        if "Already up to date." not in pull_process.stdout:
            update_message = f"Git pull successful:\n{pull_process.stdout}"

        logging.info(update_message)

        # Step 2: Docker-compose up --build
        logging.info("Restarting docker-compose service...")
        # Try with "docker compose" first for newer docker versions
        restart_process = subprocess.run(
            ["docker", "compose", "up", "-d", "--build"], capture_output=True, text=True
        )

        # If it fails, try with "docker-compose" for older versions
        if restart_process.returncode != 0:
            logging.warning(
                "`docker compose` command failed. Trying with `docker-compose`."
            )
            restart_process = subprocess.run(
                ["docker-compose", "up", "-d", "--build"],
                capture_output=True,
                text=True,
            )

        if restart_process.returncode != 0:
            logging.error(f"Docker compose restart failed: {restart_process.stderr}")
            raise HTTPException(
                status_code=500,
                detail=f"Docker compose restart failed: {restart_process.stderr}",
            )

        logging.info("Docker compose restart successful.")

        return {
            "message": f"{update_message}\nApplication update initiated successfully. The service is restarting."
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error during update process: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An internal error occurred during the update process.",
        )

@router.post("/force_poll")
async def force_poll(request: Request):
    """Manually triggers a data fetch."""
    try:
        _LOGGER.info("Manual poll triggered via API.")
        all_vehicles_data = await fetcher.run_fetch_cycle()
        if (
            hasattr(request.app.state, "mqtt_handler")
            and request.app.state.mqtt_handler
            and all_vehicles_data
        ):
            _LOGGER.info(
                f"Publishing data for {len(all_vehicles_data)} vehicles to MQTT..."
            )
            for vehicle_data in all_vehicles_data:
                request.app.state.mqtt_handler.publish(vehicle_data, autodiscovery=True)
        return {"message": "Data poll completed successfully."}
    except Exception as e:
        _LOGGER.error(f"Error during manual poll: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An internal error occurred during the data poll."
        )

@router.post("/mqtt/test")
async def mqtt_test(request: Request):
    """
    Sends the latest cached data to the MQTT broker for testing purposes.
    """
    _LOGGER.info("MQTT test message triggered via API.")

    if not hasattr(request.app.state, "mqtt_handler") or not request.app.state.mqtt_handler:
        raise HTTPException(
            status_code=400,
            detail="MQTT is not enabled or configured correctly. Please check settings.",
        )

    vehicles = await get_cached_vehicle_data()
    if not vehicles:
        raise HTTPException(
            status_code=404,
            detail="No cached vehicle data found. Please run a poll first.",
        )

    try:
        for vehicle in vehicles:
            request.app.state.mqtt_handler.publish(vehicle, autodiscovery=True)
        return {"message": "Test message sent successfully to MQTT broker."}
    except Exception as e:
        _LOGGER.error(f"Error during MQTT test publish: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while sending the MQTT message."
        )

@router.get("/credentials")
def get_stored_username():
    """API endpoint to get the stored username."""
    username = get_username()
    return {"username": username or ""}

@router.post("/credentials")
def update_credentials(creds: dict = Body(...)):
    """API endpoint to update and save credentials."""
    username = creds.get("username")
    password = creds.get("password")
    if not username or not password:
        raise HTTPException(
            status_code=400, detail="Username and password are required."
        )
    try:
        save_credentials(username, password)
        return {"message": "Credentials saved successfully."}
    except Exception as e:
        logging.error(f"Error saving credentials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to save credentials.")

@router.get("/config")
def get_config():
    """API endpoint to get the current configuration."""
    return config_manager.settings

@router.post("/config")
def update_config(new_settings: dict = Body(...)):
    """API endpoint to update and save configuration to the user-specific config file."""
    try:
        # 1. Read the existing user config to preserve unchanged settings
        try:
            with open(config_manager.user_config_path, "r") as f:
                current_user_config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            current_user_config = {}

        # 2. Deep merge the new settings from the UI into the existing user settings
        updated_user_config = config_manager._deep_merge(
            new_settings, current_user_config
        )

        # 3. Write the result back to user_config.yaml
        with open(config_manager.user_config_path, "w") as f:
            yaml.dump(updated_user_config, f, default_flow_style=False, sort_keys=False)

        # 4. Reload the configuration into memory for the running app
        config_manager.load()

        # 5. Re-apply logging settings
        setup_logging()

        return {"message": "Settings saved successfully."}
    except Exception as e:
        _LOGGER.error(f"Error updating user config file: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to write to user configuration file."
        )

@router.post("/backfill_geocoding")
async def trigger_geocoding_backfill(force_all: bool = False):
    """Triggers a manual, on-demand backfill of missing geocoding data."""
    try:
        result = await fetcher.backfill_geocoding(force_all=force_all)
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except Exception as e:
        logging.error(f"Error during manual geocoding backfill: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An internal error occurred during the geocoding backfill.",
        )

class GeocodeTestRequest(BaseModel):
    lat: float
    lon: float
    provider: str
    opencage_api_key: Optional[str] = None
    google_maps_api_key: Optional[str] = None

@router.post("/geocoding/test")
async def test_geocoding_endpoint(request: GeocodeTestRequest):
    """Tests the geocoding provider with the given credentials."""
    from app.geocoder import NominatimGeocoder, OpenCageGeocoder, GoogleMapsGeocoder

    provider = request.provider.lower()
    if provider == "opencage":
        geocoder = OpenCageGeocoder(request.opencage_api_key or "")
    elif provider == "google_maps":
        geocoder = GoogleMapsGeocoder(request.google_maps_api_key or "")
    else:
        geocoder = NominatimGeocoder()

    try:
        address = await geocoder.reverse_geocode(request.lat, request.lon)
        if address and address != "Unavailable":
            return {"address": address}
        else:
            raise HTTPException(
                status_code=400,
                detail="Could not resolve address or provider unavailable.",
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Geocoding test failed: {str(e)}")