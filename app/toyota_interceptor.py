from pytoyoda.controller import Controller

class PatchedController(Controller):
    """Intercepts raw API responses to fix missing keys before Pydantic parsing."""

    async def request_json(self, method: str, endpoint: str, **kwargs) -> dict:
        response = await super().request_json(method, endpoint, **kwargs)

        try:
            from app.fetcher import save_raw_response
            await save_raw_response(endpoint, response)
        except Exception:
            pass

        # Intercept trips endpoint to patch missing summary keys
        if "trips" in endpoint and isinstance(response, dict) and "payload" in response:
            payload = response.get("payload")
            if payload and "trips" in payload and isinstance(payload["trips"], list):
                for trip in payload["trips"]:
                    summary = trip.get("summary")
                    if isinstance(summary, dict):
                        # Inject missing keys required by pytoyoda's strict Pydantic model
                        missing_keys = [
                            "durationIdle", 
                            "countries", 
                            "maxSpeed", 
                            "lengthOverspeed", 
                            "durationOverspeed", 
                            "lengthHighway", 
                            "durationHighway"
                        ]
                        for key in missing_keys:
                            summary.setdefault(key, None)

        # Intercept vehicle/guid endpoint to patch missing remoteDisplay key
        if "vehicle/guid" in endpoint and isinstance(response, dict) and "payload" in response:
            payload = response.get("payload")
            if isinstance(payload, list):
                for vehicle in payload:
                    if isinstance(vehicle, dict):
                        vehicle.setdefault("remoteDisplay", None)

        return response
