import torch
from models.yolo import DetectionModel

# Load weights
state_dict = torch.load("cls1_yolov9_weights_only.pt", map_location="cpu")

# Create a YOLOv9 model and load the weights
model = DetectionModel(cfg="yolov9-t.yaml")
model.load_state_dict(state_dict)

# Save as a proper YOLO checkpoint
torch.save({"model": model}, "cls1_yolov9_fixed.pt")
print("✅ Model checkpoint saved: cls1_yolov9_fixed.pt")

