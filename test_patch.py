from pytoyoda.models.endpoints.vehicle_guid import VehicleGuidModel
from pydantic import ValidationError

# Print initial state
print("Before patch:")
print(VehicleGuidModel.model_fields['remote_display'].default)
print(VehicleGuidModel.model_fields['remote_display'].is_required())

# Try creating without remoteDisplay
try:
    VehicleGuidModel(vin="123", modelName="Test", role="Owner")
except ValidationError as e:
    print("Failed to create without remoteDisplay as expected.")

# Apply patch
VehicleGuidModel.model_fields['remote_display'].default = None
VehicleGuidModel.model_rebuild(force=True)

print("\nAfter patch:")
print(VehicleGuidModel.model_fields['remote_display'].default)
print(VehicleGuidModel.model_fields['remote_display'].is_required())

# Try again
try:
    v = VehicleGuidModel(vin="123", modelName="Test", role="Owner")
    print("Success! remote_display is:", v.remote_display)
except ValidationError as e:
    print("Failed to create without remoteDisplay after patch:", e)
