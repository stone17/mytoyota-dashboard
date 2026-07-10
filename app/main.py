# app/main.py
import sys
import asyncio
import json
import time
import datetime
import logging
from collections import deque
from typing import Deque, Dict

# Fix for Windows asyncio ProactorEventLoop socket shutdown errors
if sys.platform == 'win32':
    import asyncio.proactor_events
    
    def silence_proactor_connection_reset():
        _orig_call_connection_lost = asyncio.proactor_events._ProactorBasePipeTransport._call_connection_lost
        
        def _call_connection_lost(self, exc):
            try:
                _orig_call_connection_lost(self, exc)
            except (ConnectionResetError, OSError):
                pass
                
        asyncio.proactor_events._ProactorBasePipeTransport._call_connection_lost = _call_connection_lost

    silence_proactor_connection_reset()

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import fetcher
from . import database
from . import mqtt
from . import time_utils
from .config import config_manager
from .logging_config import setup_logging, TimezoneFormatter

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
    TimezoneFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
        except asyncio.CancelledError:
            _LOGGER.info("Scheduled fetch task cancelled.")
            break
        except Exception as e:
            logging.error(f"Error in scheduled fetch: {e}", exc_info=True)

        web_server_settings = config_manager.settings.get("web_server", {})
        polling_settings = web_server_settings.get("polling", {})
        mode = polling_settings.get("mode", "interval")

        try:
            if mode == "fixed_time":
                tz = time_utils.get_timezone(config_manager)
                now = datetime.datetime.now(tz)
                target_time_str = polling_settings.get("fixed_time", "07:00")
                hour, minute = map(int, target_time_str.split(":"))

                target_today = datetime.datetime(
                    now.year, now.month, now.day, hour, minute, 0, 0, tzinfo=tz
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
        except asyncio.CancelledError:
            _LOGGER.info("Scheduled fetch sleep cancelled.")
            break

async def watch_mqtt_config():
    """Watches for changes in the MQTT configuration and reloads the handler if necessary."""
    last_config = json.dumps(config_manager.settings.get("mqtt", {}), sort_keys=True)
    while True:
        try:
            await asyncio.sleep(2)
            current_config_dict = config_manager.settings.get("mqtt", {})
            current_config = json.dumps(current_config_dict, sort_keys=True)
            if current_config != last_config:
                _LOGGER.info("MQTT configuration change detected. Reloading listener...")
                last_config = current_config
                if hasattr(app.state, "mqtt_handler") and app.state.mqtt_handler:
                    app.state.mqtt_handler.stop_listener()
                    app.state.mqtt_handler.start_listener()
        except asyncio.CancelledError:
            _LOGGER.info("MQTT config watcher task cancelled.")
            break

@app.on_event("startup")
async def startup_event():
    """On startup, run an immediate fetch and then schedule periodic updates."""
    logging.info("Initializing database...")
    database.init_db()
    logging.info("Application startup...")

    app.state.bg_tasks = []

    loop = asyncio.get_running_loop()
    app.state.mqtt_handler = mqtt.MqttHandler(loop=loop)
    
    mqtt_settings = config_manager.settings.get("mqtt", {})
    if mqtt_settings.get("enabled"):
        app.state.mqtt_handler.start_listener()

    # Start the MQTT config watcher
    mqtt_task = asyncio.create_task(watch_mqtt_config())
    app.state.bg_tasks.append(mqtt_task)

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
        fetch_task = asyncio.create_task(schedule_fetch())
        app.state.bg_tasks.append(fetch_task)
    else:
        wait_time = refresh_interval - time_since_last_fetch
        logging.info(
            f"Cache is fresh. Scheduling first fetch in {int(wait_time)} seconds."
        )

        async def delayed_schedule_fetch():
            try:
                await asyncio.sleep(wait_time)
                await schedule_fetch()
            except asyncio.CancelledError:
                _LOGGER.info("Delayed fetch task cancelled.")

        delayed_task = asyncio.create_task(delayed_schedule_fetch())
        app.state.bg_tasks.append(delayed_task)

@app.on_event("shutdown")
async def shutdown_event():
    """On shutdown, gracefully disconnect the MQTT client and cancel background tasks."""
    _LOGGER.info("Application shutdown initiated...")
    
    if hasattr(app.state, "mqtt_handler") and app.state.mqtt_handler:
        app.state.mqtt_handler.stop_listener()

    if hasattr(app.state, "bg_tasks"):
        for task in app.state.bg_tasks:
            task.cancel()
        
        # Optionally wait a brief moment for tasks to acknowledge cancellation
        await asyncio.gather(*app.state.bg_tasks, return_exceptions=True)
        
    _LOGGER.info("Background tasks cancelled.")


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
