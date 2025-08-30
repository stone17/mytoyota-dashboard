# tests/test_mqtt.py
import pytest
from unittest.mock import patch, MagicMock
import json
from app.mqtt import MqttHandler

@patch("app.config.config_manager.update_and_reload")
def test_mqtt_polling_command(mock_update_reload):
    """Tests that a valid polling command calls the config update function."""
    handler = MqttHandler()
    
    mock_msg = MagicMock()
    mock_msg.topic = "mytoyota/command"
    mock_msg.payload = b'{"setting": "polling", "value": {"mode": "interval", "interval_seconds": 300}}'
    
    # Call the private method for isolated testing
    handler._on_message(None, None, mock_msg)
    
    # Assert that the config update function was called with the correct arguments
    mock_update_reload.assert_called_once_with(
        ["web_server", "polling"],
        {"mode": "interval", "interval_seconds": 300}
    )

def test_mqtt_invalid_command(caplog):
    """
    Tests that an invalid command is logged correctly and does not crash.
    The `caplog` fixture captures logging output.
    """
    handler = MqttHandler()
    
    mock_msg = MagicMock()
    mock_msg.topic = "mytoyota/command"
    mock_msg.payload = b'{"invalid": "payload"}'
    
    handler._on_message(None, None, mock_msg)
    
    assert "Invalid MQTT command payload" in caplog.text

@patch("app.mqtt.mqtt_client.Client")
def test_mqtt_publish_data(MockMqttClient):
    """
    Tests that vehicle data is correctly formatted and published for key sensors.
    This test mocks the MQTT client to prevent actual network calls.
    """
    # Arrange: Get the mock instance that will be created inside the handler
    mock_client_instance = MockMqttClient.return_value

    # Use patch.dict to temporarily set a complete MQTT config for this test
    from app.config import config_manager
    test_mqtt_config = {
        "enabled": True,
        "broker": "localhost",
        "enabled_sensors": {
            "odometer": True,
            "fuel_level": True,
            "lock_status": True
        }
    }
    with patch.dict(config_manager.settings, {"mqtt": test_mqtt_config}):
        handler = MqttHandler()
        
        vehicle_data = {
            "vin": "TESTVIN123",
            "alias": "Test RAV4",
            "dashboard": {
                "odometer": 12345,
                "fuel_level": 75,
            },
            "statistics": {"overall": {}},
            "status": { "doors": { "driver_seat": {"locked": True} } }
        }

        # Act: Call the main publish method
        handler.publish(vehicle_data)

    # Assert: Check that the client was used correctly
    mock_client_instance.connect.assert_called_once()
    mock_client_instance.loop_start.assert_called_once()

    # Assert that specific data points were published with the correct topic and payload
    mock_client_instance.publish.assert_any_call(
        "mytoyota/TESTVIN123/odometer",
        json.dumps({"value": 12345})
    )
    mock_client_instance.publish.assert_any_call(
        "mytoyota/TESTVIN123/fuel_level",
        json.dumps({"value": 75})
    )
    mock_client_instance.publish.assert_any_call(
        "mytoyota/TESTVIN123/lock_status",
        json.dumps({"value": "Locked"})
    )

    # Assert that the client was cleaned up
    mock_client_instance.loop_stop.assert_called_once()
    mock_client_instance.disconnect.assert_called_once()

def test_publish_method_logic():
    """Tests the high-level logic of the publish method, ensuring it calls helpers correctly."""
    handler = MqttHandler()
    with patch.object(handler, '_get_publisher_client') as mock_get_client, \
         patch.object(handler, '_publish_autodiscovery_configs') as mock_autodiscovery, \
         patch.object(handler, '_publish_vehicle_data') as mock_publish_data:
        
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        vehicle_data = {"vin": "TESTVIN123"}

        # Test with autodiscovery enabled
        handler.publish(vehicle_data, autodiscovery=True)
        mock_autodiscovery.assert_called_once_with(mock_client, vehicle_data)
        mock_publish_data.assert_called_once_with(mock_client, vehicle_data)

        # Reset mocks and test with autodiscovery disabled
        mock_autodiscovery.reset_mock()
        mock_publish_data.reset_mock()
        handler.publish(vehicle_data, autodiscovery=False)
        mock_autodiscovery.assert_not_called()
        mock_publish_data.assert_called_once_with(mock_client, vehicle_data)