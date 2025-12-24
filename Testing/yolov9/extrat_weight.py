import torch

# Load full checkpoint (assuming it's trusted)
ckpt = torch.load('cls1_yolov9_box.pt', map_location='cpu')

# Extract the model's state_dict (weights only)
if 'model' in ckpt:
    model_state_dict = ckpt['model'].state_dict()
    torch.save(model_state_dict, 'cls1_yolov9_weights_only.pt')
    print("✅ Weights-only checkpoint saved: cls1_yolov9_weights_only.pt")
else:
    print("❌ Error: 'model' key not found in checkpoint!")

