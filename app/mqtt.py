# app/mqtt.py
import logging
import json
from paho.mqtt import client as mqtt_client

from .config import settings

_LOGGER = logging.getLogger(__name__)

# This function remains the same
def get_client() -> mqtt_client.Client | None:
    config = settings.get("mqtt", {})
    _LOGGER.debug(f"MQTT configuration found: {config}")
    if not config.get("enabled"):
        _LOGGER.info("MQTT is not enabled in settings. Skipping connection.")
        return None
    host = config.get("host")
    if not host:
        _LOGGER.warning("MQTT is enabled, but no host is configured.")
        return None
    port = config.get("port", 1883)
    client_id = f"mytoyota-app-{hash(host)}"
    try:
        _LOGGER.info(f"Attempting to connect to MQTT broker at {host}:{port}...")
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
        if config.get("username") and config.get("password"):
            _LOGGER.debug("Using username and password for MQTT connection.")
            client.username_pw_set(config.get("username"), config.get("password"))
        client.connect(host, port)
        client.loop_start()
        _LOGGER.info(f"Successfully connected to MQTT broker at {host}:{port}")
        return client
    except Exception as e:
        _LOGGER.error(f"Failed to connect to MQTT broker: {e}", exc_info=True)
        return None


def publish_autodiscovery_configs(client: mqtt_client.Client, vehicle_data: dict):
    """
    Publishes the configuration messages for MQTT Auto Discovery.
    This tells Domoticz/Home Assistant how to create the devices.
    """
    config = settings.get("mqtt", {})
    vin = vehicle_data.get("vin")
    if not vin:
        return

    _LOGGER.info(f"Publishing MQTT auto-discovery configs for VIN {vin}...")
    
    discovery_prefix = config.get("discovery_prefix", "homeassistant")
    base_topic = config.get("base_topic", "mytoyota/{vin}").format(vin=vin)
    enabled_sensors = config.get("enabled_sensors", {})
    
    unit_system = settings.get("unit_system", "metric")
    is_imperial = unit_system.startswith("imperial")
    
    odom_unit = "mi" if is_imperial else "km"
    consump_unit = "MPG" if is_imperial else "L/100km"
    range_unit = "mi" if is_imperial else "km"

    device_info = {
        "identifiers": [vin],
        "name": vehicle_data.get("alias", f"Toyota {vin}"),
        "model": vehicle_data.get("model_name", "Unknown"),
        "manufacturer": "Toyota"
    }

    # Define all possible sensors
    all_sensors = {
        "odometer": {
            "component": "sensor", "name": "Odometer", "unit_of_measurement": odom_unit, "icon": "mdi:counter",
            "value_template": "{{ value_json.value | int }}"
        },
        "fuel_level": {
            "component": "sensor", "name": "Fuel Level", "unit_of_measurement": "%", "icon": "mdi:gas-station",
            "value_template": "{{ value_json.value | int }}"
        },
        "fuel_consumption": {
            "component": "sensor", "name": "Fuel Consumption", "unit_of_measurement": consump_unit, "icon": "mdi:fuel",
            "value_template": "{{ value_json.value | float(2) }}"
        },
        "lock_status": {
            "component": "binary_sensor", "name": "Lock Status", "device_class": "lock",
            "payload_on": "UNLOCKED", "payload_off": "LOCKED"
        },
        "total_range": {
            "component": "sensor", "name": "Total Range", "unit_of_measurement": range_unit, "icon": "mdi:map-marker-distance",
            "value_template": "{{ value_json.value | int }}"
        },
        "battery_level": {
            "component": "sensor", "name": "EV Battery", "unit_of_measurement": "%", "icon": "mdi:battery", "device_class": "battery",
            "value_template": "{{ value_json.value | int }}"
        },
        "ev_range": {
            "component": "sensor", "name": "EV Range", "unit_of_measurement": range_unit, "icon": "mdi:map-marker-distance",
            "value_template": "{{ value_json.value | int }}"
        }
    }

    for sensor_key, sensor_config in all_sensors.items():
        # Only publish config if the sensor is enabled in settings
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

        _LOGGER.debug(f"Publishing discovery config to '{config_topic}': {json.dumps(payload)}")
        client.publish(config_topic, json.dumps(payload), retain=True)

def publish_vehicle_data(client: mqtt_client.Client, vehicle_data: dict):
    """
    Publishes key vehicle statistics to the broker using an existing client.
    """
    _LOGGER.info(f"Preparing to publish MQTT data for VIN: {vehicle_data.get('vin')}")
    config = settings.get("mqtt", {})
    try:
        vin = vehicle_data.get("vin")
        if not vin:
            _LOGGER.warning("Cannot publish MQTT data, VIN not found in vehicle data.")
            return
        base_topic = config.get("base_topic", "mytoyota/{vin}").format(vin=vin)
        enabled_sensors = config.get("enabled_sensors", {})
        
        unit_system = settings.get("unit_system", "metric")
        is_imperial = unit_system.startswith("imperial")
        KM_TO_MI = 0.621371

        # Helper function for logging
        def log_skip(sensor_name):
            _LOGGER.debug(f"Skipping MQTT publish for '{sensor_name}' because its value is missing from the API data.")

        # Publish Odometer
        if enabled_sensors.get("odometer", False):
            odometer_km = vehicle_data.get("dashboard", {}).get("odometer")
            if odometer_km is not None:
                odom_value = round(odometer_km * KM_TO_MI) if is_imperial else round(odometer_km)
                client.publish(f"{base_topic}/odometer", json.dumps({"value": odom_value}))
            else:
                log_skip("odometer")

        # Publish Lock Status
        if enabled_sensors.get("lock_status", False):
            status = vehicle_data.get("status", {})
            # Lock status is calculated, so it should always have a value.
            all_locked = all(door.get("locked") for door in status.get("doors", {}).values()) if status.get("doors") else False
            lock_payload = "LOCKED" if all_locked else "UNLOCKED"
            client.publish(f"{base_topic}/lock_status", lock_payload)

        # Publish Fuel Level
        if enabled_sensors.get("fuel_level", False):
            fuel_level = vehicle_data.get("dashboard", {}).get("fuel_level")
            if fuel_level is not None:
                client.publish(f"{base_topic}/fuel_level", json.dumps({"value": fuel_level}))
            else:
                log_skip("fuel_level")

        # Publish Fuel Consumption
        if enabled_sensors.get("fuel_consumption", False):
            consumption_l100km = vehicle_data.get("statistics", {}).get("overall", {}).get("fuel_consumption_l_100km")
            if consumption_l100km is not None:
                consump_value = consumption_l100km
                if is_imperial and consumption_l100km > 0:
                    mpg_factor = 282.481 if unit_system == "imperial_uk" else 235.214
                    consump_value = mpg_factor / consumption_l100km
                client.publish(f"{base_topic}/fuel_consumption", json.dumps({"value": consump_value}))
            else:
                log_skip("fuel_consumption")
        
        # Publish Total Range
        if enabled_sensors.get("total_range", False):
            range_km = vehicle_data.get("dashboard", {}).get("total_range")
            if range_km is not None:
                range_value = round(range_km * KM_TO_MI) if is_imperial else round(range_km)
                client.publish(f"{base_topic}/total_range", json.dumps({"value": range_value}))
            else:
                log_skip("total_range")
        
        # Publish EV Battery Level
        if enabled_sensors.get("battery_level", False):
            battery_level = vehicle_data.get("dashboard", {}).get("battery_level")
            if battery_level is not None:
                client.publish(f"{base_topic}/battery_level", json.dumps({"value": battery_level}))
            else:
                log_skip("battery_level")

        # Publish EV Range
        if enabled_sensors.get("ev_range", False):
            ev_range_km = vehicle_data.get("dashboard", {}).get("battery_range")
            if ev_range_km is not None:
                ev_range_value = round(ev_range_km * KM_TO_MI) if is_imperial else round(ev_range_km)
                client.publish(f"{base_topic}/ev_range", json.dumps({"value": ev_range_value}))
            else:
                log_skip("ev_range")
        
        _LOGGER.info(f"Finished publishing data for VIN {vin}")
    except Exception as e:
        _LOGGER.error(f"Error publishing vehicle data to MQTT for VIN {vin}: {e}", exc_info=True)


def disconnect(client: mqtt_client.Client):
    if client:
        client.loop_stop()
        client.disconnect()
        _LOGGER.info("Disconnected from MQTT broker.")