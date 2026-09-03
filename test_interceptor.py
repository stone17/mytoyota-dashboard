import asyncio
from app.toyota_interceptor import PatchedController

async def test_interceptor():
    controller = PatchedController(username="test", password="test")
    
    # Mock super().request_json
    async def mock_request_json(self, method, endpoint, **kwargs):
        return {
            "status": {"messages": [{"responseCode": "ONE-VL-10000"}]},
            "payload": [{"vin": "123", "modelName": "Test"}]
        }
    
    # Patch the super method for testing
    controller.__class__.__bases__[0].request_json = mock_request_json
    
    response = await controller.request_json("GET", "/v2/vehicle/guid")
    
    print(response)
    if "remoteDisplay" in response["payload"][0]:
        print("Success: remoteDisplay was injected!")
        print("Value:", response["payload"][0]["remoteDisplay"])
    else:
        print("Failure: remoteDisplay was not injected.")

asyncio.run(test_interceptor())
