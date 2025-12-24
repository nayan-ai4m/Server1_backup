import torch
from models.yolo import DetectionModel  # Ensure this is the correct import path

torch.serialization.add_safe_globals([DetectionModel])
ckpt = torch.load('cls1_yolov9_box.pt', map_location='cpu', weights_only=True)
print(ckpt.keys())

