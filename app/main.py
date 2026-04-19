# app/main.py
import asyncio
import json
import time
import datetime
import logging
from collections import deque
from typing import Deque, Dict

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import fetcher
from . import database
from . import mqtt
from .config import config_manager
from .logging_config import setup_logging

# Import the new routers
from .routers import pages, trips, vehicles, system

# Configure logging at the very beginning of the application startup
setup_logging()
_LOGGER = logging.getLogger(__name__)

# --- Live Log Streaming Setup ---
log_history_size = config_manager.settings.get("log_history_size", 200)
log_history: Deque[Dict] = deque(maxlen=log_history_size)
log_queue = asyncio.Queue()

class WebLogHandler(logging.Handler):
    """A custom logging handler that captures logs for the web UI."""
    def emit(self, record):
        log_entry = {"level": record.levelname, "message": self.format(record)}
        log_history.append(log_entry)
        try:
            log_queue.put_nowait(log_entry)
        except asyncio.QueueFull:
            pass

web_log_handler = WebLogHandler()
web_log_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logging.getLogger().addHandler(web_log_handler)

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include the routers
app.include_router(pages.router)
app.include_router(trips.router)
app.include_router(vehicles.router)
app.include_router(system.router)

async def schedule_fetch():
    """Runs the data fetcher on a schedule."""
    while True:
        try:
            all_vehicles_data = await fetcher.run_fetch_cycle()

            if (
                hasattr(app.state, "mqtt_handler")
                and app.state.mqtt_handler
                and all_vehicles_data
            ):
                _LOGGER.info(
                    f"Publishing data for {len(all_vehicles_data)} vehicles to MQTT..."
                )
                for vehicle_data in all_vehicles_data:
                    app.state.mqtt_handler.publish(vehicle_data, autodiscovery=True)
        except Exception as e:
            logging.error(f"Error in scheduled fetch: {e}", exc_info=True)

        web_server_settings = config_manager.settings.get("web_server", {})
        polling_settings = web_server_settings.get("polling", {})
        mode = polling_settings.get("mode", "interval")

        if mode == "fixed_time":
            now = datetime.datetime.now()
            target_time_str = polling_settings.get("fixed_time", "07:00")
            hour, minute = map(int, target_time_str.split(":"))

            target_today = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

            if now >= target_today:
                target_next = target_today + datetime.timedelta(days=1)
            else:
                target_next = target_today

            sleep_duration = (target_next - now).total_seconds()
            logging.info(
                f"Next poll scheduled for {target_next}. Sleeping for {int(sleep_duration)} seconds."
            )
            await asyncio.sleep(sleep_duration)
        else:
            interval = polling_settings.get(
                "interval_seconds"
            ) or web_server_settings.get("data_refresh_interval_seconds", 3600)
            logging.info(f"Next poll in {interval} seconds.")
            await asyncio.sleep(interval)

@app.on_event("startup")
async def startup_event():
    """On startup, run an immediate fetch and then schedule periodic updates."""
    logging.info("Initializing database...")
    database.init_db()
    logging.info("Application startup...")

    mqtt_settings = config_manager.settings.get("mqtt", {})
    if mqtt_settings.get("enabled"):
        loop = asyncio.get_running_loop()
        app.state.mqtt_handler = mqtt.MqttHandler(loop=loop)
        app.state.mqtt_handler.start_listener()

    web_server_settings = config_manager.settings.get("web_server", {})
    polling_settings = web_server_settings.get("polling", {})
    refresh_interval = polling_settings.get(
        "interval_seconds"
    ) or web_server_settings.get("data_refresh_interval_seconds", 3600)
    time_since_last_fetch = float("inf")

    if fetcher.CACHE_FILE.exists():
        last_modified_time = fetcher.CACHE_FILE.stat().st_mtime
        time_since_last_fetch = time.time() - last_modified_time

    if time_since_last_fetch >= refresh_interval:
        logging.info("Cache is stale or missing. Triggering immediate data fetch.")
        asyncio.create_task(schedule_fetch())
    else:
        wait_time = refresh_interval - time_since_last_fetch
        logging.info(
            f"Cache is fresh. Scheduling first fetch in {int(wait_time)} seconds."
        )

        async def delayed_schedule_fetch():
            await asyncio.sleep(wait_time)
            await schedule_fetch()

        asyncio.create_task(delayed_schedule_fetch())

@app.on_event("shutdown")
def shutdown_event():
    """On shutdown, gracefully disconnect the MQTT client."""
    if hasattr(app.state, "mqtt_handler") and app.state.mqtt_handler:
        app.state.mqtt_handler.stop_listener()


async def log_stream_generator(request: Request):
    """Yields historical logs as a single batch, then streams live log messages as Server-Sent Events."""
    initial_history = list(log_history)
    if initial_history:
        yield f"event: history\ndata: {json.dumps(initial_history)}\n\n"

    while True:
        if await request.is_disconnected():
            _LOGGER.debug("Log stream client disconnected.")
            break
        try:
            log_entry = await asyncio.wait_for(log_queue.get(), timeout=30)
            yield f"event: message\ndata: {json.dumps(log_entry)}\n\n"
            log_queue.task_done()
        except asyncio.TimeoutError:
            yield ": keep-alive\n\n"

@app.get("/api/logs")
async def stream_logs(request: Request):
    """API endpoint to stream logs using Server-Sent Events (SSE)."""
    return StreamingResponse(
        log_stream_generator(request), media_type="text/event-stream"
    )