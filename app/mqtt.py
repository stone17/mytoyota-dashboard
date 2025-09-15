# app/mqtt.py
import logging
import asyncio
import os
import json
from paho.mqtt import client as mqtt_client
from typing import Optional

from .config import config_manager

_LOGGER = logging.getLogger(__name__)

class MqttHandler:
    """A class to manage all MQTT interactions, including publishing and listening for commands."""

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        """Initializes the MqttHandler."""
        self.listener_client: Optional[mqtt_client.Client] = None
        self.loop = loop
        _LOGGER.info("MqttHandler initialized.")

    # --- Command Listener Methods ---

    def _on_message(self, client, userdata, msg):
        """Callback for handling incoming MQTT messages for commands."""
        # Unconditionally log that a message was received for better debugging
        _LOGGER.info(f"MQTT message received on topic: '{msg.topic}'")
        # Always use the latest config
        current_mqtt_config = config_manager.settings.get("mqtt", {})
        # Use a dedicated command topic to avoid conflicts with vehicle-specific base topics.
        command_topic = current_mqtt_config.get("command_topic", "mytoyota/command")

        if msg.topic == command_topic:
            try:
                _LOGGER.info(f"Processing command with payload: {msg.payload.decode()}")
                payload = json.loads(msg.payload.decode())

                # Handle action-based commands (like force_poll)
                command = payload.get("command")
                if command == "force_poll":
                    if self.loop:
                        _LOGGER.info("Scheduling a force poll due to MQTT command.")
                        asyncio.run_coroutine_threadsafe(self._run_poll_and_publish(), self.loop)
                    else:
                        _LOGGER.error("Cannot trigger force_poll via MQTT: event loop not available to MqttHandler.")
                    return # Command handled

                # Handle settings-based commands
                setting = payload.get("setting")
                value = payload.get("value")

                if not setting:
                    _LOGGER.error("Invalid MQTT command payload. Must contain either a 'command' (e.g., 'force_poll') or a 'setting' to modify.")
                    return

                if not isinstance(value, dict):
                    _LOGGER.error(f"Invalid MQTT command payload for setting '{setting}'. The 'value' must be an object.")
                    return

                if setting == "polling":
                    if "mode" in value and value["mode"] in ["interval", "fixed_time"]:
                        config_manager.update_and_reload(["web_server", "polling"], value)
                    else:
                        _LOGGER.error(f"Invalid 'mode' for polling setting in MQTT command: {value.get('mode')}")
                else:
                    _LOGGER.warning(f"Received command for unsupported setting: {setting}")

            except json.JSONDecodeError:
                _LOGGER.error("Failed to decode MQTT command payload as JSON.")
            except Exception as e:
                _LOGGER.error(f"Error processing MQTT command: {e}", exc_info=True)

    async def _run_poll_and_publish(self):
        """Async helper to run a fetch cycle and publish results. To be called from the event loop."""
        from . import fetcher  # Local import to avoid circular dependency
        _LOGGER.info("Manual poll triggered via MQTT command.")
        try:
            all_vehicles_data = await fetcher.run_fetch_cycle()
            if all_vehicles_data:
                _LOGGER.info(f"Publishing data for {len(all_vehicles_data)} vehicles to MQTT after poll...")
                for vehicle_data in all_vehicles_data:
                    # `self` is the MqttHandler instance
                    self.publish(vehicle_data, autodiscovery=True)
        except Exception as e:
            _LOGGER.error(f"Error during MQTT-triggered poll: {e}", exc_info=True)

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for MQTT connection."""
        if rc == 0:
            _LOGGER.info("MQTT command listener connected successfully.")
            # Always use the latest config
            current_mqtt_config = config_manager.settings.get("mqtt", {})
            # Use a dedicated command topic.
            command_topic = current_mqtt_config.get("command_topic", "mytoyota/command")
            client.subscribe(command_topic)
            _LOGGER.info(f"Subscribed to MQTT command topic: {command_topic}")
        else:
            _LOGGER.error(f"Failed to connect MQTT command listener, return code {rc}\n")

    def start_listener(self):
        """Sets up and starts the persistent MQTT client for listening to commands."""
        # Always use the latest config
        current_mqtt_config = config_manager.settings.get("mqtt", {})
        if not current_mqtt_config.get("enabled"):
            return
        try:
            _LOGGER.info("Setting up persistent MQTT client for commands...")
            client_id = f"mytoyota-dashboard-listener-{os.getpid()}"
            self.listener_client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id=client_id)
            
            if current_mqtt_config.get("username") and current_mqtt_config.get("password"):
                self.listener_client.username_pw_set(current_mqtt_config.get("username"), current_mqtt_config.get("password"))
            
            self.listener_client.on_connect = self._on_connect
            self.listener_client.on_message = self._on_message
            
            # Use broker from config, not host
            broker = current_mqtt_config.get("broker") or current_mqtt_config.get("host")
            if not broker:
                _LOGGER.error("MQTT is enabled, but no broker/host is configured.")
                return
            
            self.listener_client.connect_async(broker, current_mqtt_config.get("port", 1883), 60)
            self.listener_client.loop_start()
        except Exception as e:
            _LOGGER.error(f"Failed to setup persistent MQTT client: {e}", exc_info=True)
            self.listener_client = None

    def stop_listener(self):
        """Stops the MQTT command listener gracefully."""
        if self.listener_client:
            _LOGGER.info("Stopping MQTT command listener.")
            self.listener_client.loop_stop()
            self.listener_client.disconnect()

    # --- Publishing Methods ---

    def _get_publisher_client(self) -> Optional[mqtt_client.Client]:
        """Gets a temporary, connected client for publishing data."""
        # Always use the latest config
        current_mqtt_config = config_manager.settings.get("mqtt", {})
        if not current_mqtt_config.get("enabled"):
            _LOGGER.info("MQTT is not enabled in settings. Skipping publish.")
            return None
        
        broker = current_mqtt_config.get("broker") or current_mqtt_config.get("host")
        if not broker:
            _LOGGER.warning("MQTT is enabled, but no broker/host is configured.")
            return None
        
        port = current_mqtt_config.get("port", 1883)
        client_id = f"mytoyota-app-publisher-{os.getpid()}"
        try:
            client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
            if current_mqtt_config.get("username") and current_mqtt_config.get("password"):
                client.username_pw_set(current_mqtt_config.get("username"), current_mqtt_config.get("password"))
            client.connect(broker, port)
            client.loop_start()
            return client
        except Exception as e:
            _LOGGER.error(f"Failed to connect to MQTT broker for publishing: {e}", exc_info=True)
            return None

    def _publish_autodiscovery_configs(self, client: mqtt_client.Client, vehicle_data: dict):
        vin = vehicle_data.get("vin")
        if not vin:
            return

        _LOGGER.info(f"Publishing MQTT auto-discovery configs for VIN {vin}...")
        # Always use the latest config
        current_mqtt_config = config_manager.settings.get("mqtt", {})
        discovery_prefix = current_mqtt_config.get("discovery_prefix", "homeassistant")
        base_topic = current_mqtt_config.get("base_topic", "mytoyota/{vin}").format(vin=vin)
        enabled_sensors = current_mqtt_config.get("enabled_sensors", {})
        
        unit_system =  config_manager.settings.get("unit_system", "metric")
        is_imperial = unit_system.startswith("imperial")
        
        consump_unit = "MPG" if is_imperial else "L/100km"
        range_unit = "mi" if is_imperial else "km"

        device_info = {
            "identifiers": [vin],
            "name": vehicle_data.get("alias", f"Toyota {vin}"),
            "model": vehicle_data.get("model_name", "Unknown"),
            "manufacturer": "Toyota"
        }

        all_sensors = {
            "odometer": {"component": "sensor", "name": "Odometer", "unit_of_measurement": range_unit, "icon": "mdi:counter", "value_template": "{{ value_json.value | int }}"},
            "fuel_level": {"component": "sensor", "name": "Fuel Level", "unit_of_measurement": "%", "icon": "mdi:gas-station", "value_template": "{{ value_json.value | int }}"},
            "fuel_consumption": {"component": "sensor", "name": "Fuel Consumption", "unit_of_measurement": consump_unit, "icon": "mdi:fuel", "value_template": "{{ value_json.value | float(2) }}"},
            "lock_status": {"component": "sensor", "name": "Lock Status", "icon": "mdi:lock", "value_template": "{{ value_json.value }}"},
            "total_range": {"component": "sensor", "name": "Total Range", "unit_of_measurement": range_unit, "icon": "mdi:map-marker-distance", "value_template": "{{ value_json.value | int }}"},
            "battery_level": {"component": "sensor", "name": "EV Battery", "unit_of_measurement": "%", "icon": "mdi:battery", "device_class": "battery", "value_template": "{{ value_json.value | int }}"},
            "ev_range": {"component": "sensor", "name": "EV Range", "unit_of_measurement": range_unit, "icon": "mdi:map-marker-distance", "value_template": "{{ value_json.value | int }}"},
            "score": {"component": "sensor", "name": "Global Score", "unit_of_measurement": "%", "icon": "mdi:star-circle-outline", "value_template": "{{ value_json.value | int }}"},
            "location_lat_long": {"component": "sensor", "name": "Location Lat/Long", "icon": "mdi:map-marker", "value_template": "{{ value_json.value }}"},
            "location": {"component": "sensor", "name": "Location Address", "icon": "mdi:map-marker", "value_template": "{{ value_json.value }}"},
            "highway_distance": {"component": "sensor", "name": "Total Highway Distance", "unit_of_measurement": range_unit, "icon": "mdi:road-variant", "value_template": "{{ value_json.value | int }}"},
            "total_ev_distance": {"component": "sensor", "name": "Total EV Distance", "unit_of_measurement": range_unit, "icon": "mdi:leaf", "value_template": "{{ value_json.value | int }}"}
        }

        for sensor_key, sensor_config in all_sensors.items():
            if not enabled_sensors.get(sensor_key, False):
                continue

            component = sensor_config["component"]
            unique_id = f"{vin}_{sensor_key}"
            config_topic = f"{discovery_prefix}/{component}/{unique_id}/config"
            
            payload = {
                "name": f"{device_info['name']} {sensor_config['name']}",
                "unique_id": unique_id,
                "state_topic": f"{base_topic}/{sensor_key}",
                "device": device_info,
                **{k: v for k, v in sensor_config.items() if k not in ["component", "name"]}
            }

            if not payload.get("unit_of_measurement"):
                payload.pop("unit_of_measurement", None)

            client.publish(config_topic, json.dumps(payload), retain=True)

    def _publish_vehicle_data(self, client: mqtt_client.Client, vehicle_data: dict):
        try:
            vin = vehicle_data.get("vin")
            if not vin:
                _LOGGER.warning("Cannot publish MQTT data, VIN not found in vehicle data.")
                return
            current_mqtt_config = config_manager.settings.get("mqtt", {})
            base_topic = current_mqtt_config.get("base_topic", "mytoyota/{vin}").format(vin=vin)
            enabled_sensors = current_mqtt_config.get("enabled_sensors", {})
            
            unit_system =  config_manager.settings.get("unit_system", "metric")
            is_imperial = unit_system.startswith("imperial")
            KM_TO_MI = 0.621371

            dashboard = vehicle_data.get("dashboard", {})
            overall_stats = vehicle_data.get("statistics", {}).get("overall", {})

            def log_skip(sensor_name):
                _LOGGER.debug(f"Skipping MQTT publish for '{sensor_name}' because its value is missing from the API data.")

            if enabled_sensors.get("odometer", False):
                odometer_km = dashboard.get("odometer")
                if odometer_km is not None:
                    odom_value = round(odometer_km * KM_TO_MI) if is_imperial else round(odometer_km)
                    client.publish(f"{base_topic}/odometer", json.dumps({"value": odom_value}))
                else: log_skip("odometer")
            
            if enabled_sensors.get("lock_status", False):
                status = vehicle_data.get("status", {})
                all_locked = all(door.get("locked") for door in status.get("doors", {}).values()) if status.get("doors") else False
                lock_payload = "Locked" if all_locked else "Open"
                client.publish(f"{base_topic}/lock_status", json.dumps({"value": lock_payload}))

            if enabled_sensors.get("fuel_level", False):
                fuel_level = dashboard.get("fuel_level")
                if fuel_level is not None: client.publish(f"{base_topic}/fuel_level", json.dumps({"value": fuel_level}))
                else: log_skip("fuel_level")

            if enabled_sensors.get("fuel_consumption", False):
                consumption_l100km = overall_stats.get("fuel_consumption_l_100km")
                if consumption_l100km is not None:
                    consump_value = consumption_l100km
                    if is_imperial and consumption_l100km > 0:
                        mpg_factor = 282.481 if unit_system == "imperial_uk" else 235.214
                        consump_value = mpg_factor / consumption_l100km
                    client.publish(f"{base_topic}/fuel_consumption", json.dumps({"value": consump_value}))
                else: log_skip("fuel_consumption")
            
            if enabled_sensors.get("total_range", False):
                range_km = dashboard.get("total_range")
                if range_km is not None:
                    range_value = round(range_km * KM_TO_MI) if is_imperial else round(range_km)
                    client.publish(f"{base_topic}/total_range", json.dumps({"value": range_value}))
                else: log_skip("total_range")
            
            if enabled_sensors.get("battery_level", False):
                battery_level = dashboard.get("battery_level")
                if battery_level is not None: client.publish(f"{base_topic}/battery_level", json.dumps({"value": battery_level}))
                else: log_skip("battery_level")

            if enabled_sensors.get("ev_range", False):
                ev_range_km = dashboard.get("battery_range")
                if ev_range_km is not None:
                    ev_range_value = round(ev_range_km * KM_TO_MI) if is_imperial else round(ev_range_km)
                    client.publish(f"{base_topic}/ev_range", json.dumps({"value": ev_range_value}))
                else: log_skip("ev_range")

            if enabled_sensors.get("score", False):
                score = overall_stats.get("score_global")
                if score is not None: client.publish(f"{base_topic}/score", json.dumps({"value": score}))
                else: log_skip("score")
            
            if enabled_sensors.get("location_lat_long", False):
                lat = dashboard.get("latitude")
                lon = dashboard.get("longitude")
                if lat is not None and lon is not None: client.publish(f"{base_topic}/location_lat_long", json.dumps({"value": f"{lat}, {lon}"}))
                else: log_skip("location_lat_long")

            if enabled_sensors.get("location", False):
                address = dashboard.get("address")
                if address and address != "Unavailable": client.publish(f"{base_topic}/location", json.dumps({"value": address}))
                else: log_skip("location")
            
            if enabled_sensors.get("highway_distance", False):
                highway_dist_km = overall_stats.get("total_highway_distance_km")
                if highway_dist_km is not None:
                    dist_value = round(highway_dist_km * KM_TO_MI) if is_imperial else round(highway_dist_km)
                    client.publish(f"{base_topic}/highway_distance", json.dumps({"value": dist_value}))
                else: log_skip("highway_distance")

            if enabled_sensors.get("total_ev_distance", False):
                ev_dist_km = overall_stats.get("total_ev_distance_km")
                if ev_dist_km is not None:
                    dist_value = round(ev_dist_km * KM_TO_MI) if is_imperial else round(ev_dist_km)
                    client.publish(f"{base_topic}/total_ev_distance", json.dumps({"value": dist_value}))
                else: log_skip("total_ev_distance")

            _LOGGER.info(f"Finished publishing data for VIN {vin}")
        except Exception as e:
            _LOGGER.error(f"Error publishing vehicle data to MQTT for VIN {vin}: {e}", exc_info=True)

    def publish(self, vehicle_data: dict, autodiscovery: bool = False):
        """
        Connects to the MQTT broker, publishes vehicle data, and disconnects.
        Optionally publishes Home Assistant auto-discovery configuration.
        """
        client = self._get_publisher_client()
        if not client:
            return

        try:
            if autodiscovery:
                self._publish_autodiscovery_configs(client, vehicle_data)
            self._publish_vehicle_data(client, vehicle_data)
        finally:
            client.loop_stop()
            client.disconnect()
            _LOGGER.debug("Disconnected from MQTT broker after publishing.")