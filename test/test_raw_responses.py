import pytest
import json
from app.config import config_manager
from app.fetcher import save_raw_response, save_poll_metadata, current_poll_id, current_poll_updates

@pytest.mark.asyncio
async def test_save_raw_response_and_metadata(tmp_path):
    # Mock DATA_DIR in config to use our temp path
    import app.fetcher
    import app.config
    
    original_data_dir = app.fetcher.DATA_DIR
    app.fetcher.DATA_DIR = tmp_path
    app.config.DATA_DIR = tmp_path

    # Enable raw response saving in settings
    config_manager.settings["save_raw_responses"] = True
    config_manager.settings["raw_responses_retention"] = "always"

    try:
        # Set context variables
        poll_id = "20260714_120000_test"
        current_poll_id.set(poll_id)
        current_poll_updates.set({"trips": [{"id": "trip1"}], "status_updated": True})

        # Call save_raw_response
        endpoint = "trips"
        test_response = {"payload": {"trips": []}}
        await save_raw_response(endpoint, test_response)

        # Call save_poll_metadata
        await save_poll_metadata()

        # Check if directory and files were created
        poll_dir = tmp_path / "raw_responses" / poll_id
        assert poll_dir.exists()

        # Check raw response file
        json_files = list(poll_dir.glob("*_trips.json"))
        assert len(json_files) == 1
        with open(json_files[0], "r") as f:
            saved_data = json.load(f)
        assert saved_data == test_response

        # Check metadata file
        metadata_file = poll_dir / "metadata.json"
        assert metadata_file.exists()
        with open(metadata_file, "r") as f:
            saved_metadata = json.load(f)
        assert saved_metadata["poll_id"] == poll_id
        assert saved_metadata["updates"]["status_updated"] is True

    finally:
        # Restore original DATA_DIR
        app.fetcher.DATA_DIR = original_data_dir
        app.config.DATA_DIR = original_data_dir
        config_manager.settings["save_raw_responses"] = False

@pytest.mark.asyncio
async def test_get_raw_response_endpoint(client, test_data_dir):
    config_manager.settings["save_raw_responses"] = True
    
    poll_id = "20260714_120000_test"
    filename = "20260714_120000_v1_trips.json"
    poll_dir = test_data_dir / "raw_responses" / poll_id
    poll_dir.mkdir(parents=True, exist_ok=True)
    
    test_data = {"payload": {"trips": [{"id": "trip1"}]}}
    with open(poll_dir / filename, "w") as f:
        json.dump(test_data, f)
        
    response = await client.get(f"/api/raw_responses/{poll_id}/{filename}")
    assert response.status_code == 200
    assert response.json() == test_data

@pytest.mark.asyncio
async def test_raw_responses_path_traversal_protection(client, test_data_dir):
    # Test poll_id traversal
    response1 = await client.get("/api/raw_responses/../download")
    assert response1.status_code in (400, 404)
    
    response2 = await client.get("/api/raw_responses/..%2F..%2F/download")
    assert response2.status_code in (400, 404)
    
    # Test filename traversal
    poll_id = "20260714_120000_test"
    response3 = await client.get(f"/api/raw_responses/{poll_id}/..%2F..%2Fconfig.yaml")
    assert response3.status_code in (400, 404)
