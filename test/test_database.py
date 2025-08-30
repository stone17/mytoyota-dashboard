# tests/test_database.py
import pytest
from app import database
import datetime

class MockAPITrip:
    """A mock object that mimics the structure of a trip from the pytoyoda library."""
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.vin = kwargs.get("vin")
        self.start_timestamp = kwargs.get("start_timestamp")
        self.end_timestamp = kwargs.get("end_timestamp")
        self.start_address = kwargs.get("start_address")
        self.end_address = kwargs.get("end_address")
        self.distance_km = kwargs.get("distance_km")
        self.fuel_consumption_l_100km = kwargs.get("fuel_consumption_l_100km")
        # Add any other attributes that insert_trips uses

def test_insert_and_get_trips(test_db_session):
    """
    Tests that trips can be inserted into and retrieved from the database.
    The test_db_session fixture ensures this runs on a temporary in-memory DB.
    """
    trip_data = {
            "id": 12345, # Reverted to integer to match the reverted model
        "vin": "TESTVIN123",
        "start_timestamp": datetime.datetime.now(datetime.timezone.utc),
        "end_timestamp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10),
        "distance_km": 15.5,
        "fuel_consumption_l_100km": 5.5
    }
    trips_to_insert = [MockAPITrip(**trip_data)]

    database.insert_trips(trips_to_insert)
    retrieved_trips = database.get_all_trips()

    assert len(retrieved_trips) == 1
    assert retrieved_trips[0]["vin"] == "TESTVIN123"
    assert retrieved_trips[0]["distance_km"] == 15.5