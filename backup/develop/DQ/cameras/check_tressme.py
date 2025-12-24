import numpy as np
import os
import cv2
import tritonclient.grpc as grpcclient
from tritonclient.utils import InferenceServerException
from tqdm import tqdm
import sys

# Hardcoded paths
INPUT_FOLDER = "tressme_test"
OUTPUT_FOLDER = "tressme_output"
TRITON_SERVER_URL = "localhost:8006"
MODEL_NAME = "red_tape"

class TritonDetectionClient:
    def __init__(self, url: str, model_name: str, model_version: str = '1') -> None:
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.model_version = model_version
        self.input_height = 960
        self.input_width = 960
        self.conf_threshold = 0.60
        self.output_names = self.verify_model_metadata()

    def verify_model_metadata(self):
        try:
            metadata = self.client.get_model_metadata(self.model_name)
            output_names = [output.name for output in metadata.outputs]
            print(f"Model outputs: {output_names}")
            return output_names
        except InferenceServerException as e:
            print(f"Failed to get model metadata: {e}")
            sys.exit(1)

    def prepare_input(self, image):
        self.img_height, self.img_width = image.shape[:2]
        img_resized = cv2.resize(image, (self.input_width, self.input_height))
        img_rgb = cv2.cvtColor(img_resized, cv2.COLOR_BGR2RGB)
        img_normalized = img_rgb / 255.0
        input_tensor = img_normalized.transpose(2, 0, 1)[np.newaxis, ...].astype(np.float32)
        return input_tensor

    def infer(self, image):
        input_tensor = self.prepare_input(image)
        inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]
        inputs[0].set_data_from_numpy(input_tensor)
        
        # Use the actual output names from the model
        outputs = [grpcclient.InferRequestedOutput(name) for name in self.output_names]
        
        try:
            results = self.client.infer(
                model_name=self.model_name,
                model_version=self.model_version,
                inputs=inputs,
                outputs=outputs
            )
            
            # Get outputs dynamically based on model's output names
            output_data = {}
            for name in self.output_names:
                output_data[name] = results.as_numpy(name)
            
            return self.process_output(output_data, image)
            
        except Exception as e:
            print(f"Inference error: {str(e)}")
            return [], [], [], image

    def process_output(self, output_data, image):
        # Adjust this based on your model's actual output structure
        # This is just an example - you'll need to modify based on your model's outputs
        if 'boxes' in output_data and 'scores' in output_data and 'classes' in output_data:
            boxes = output_data['boxes']
            scores = output_data['scores']
            class_ids = output_data['classes']
        else:
            # Try to handle common output name variations
            boxes = output_data.get('detection_boxes', output_data.get('boxes', np.empty((0, 4))))
            scores = output_data.get('detection_scores', output_data.get('scores', np.empty(0)))
            class_ids = output_data.get('detection_classes', output_data.get('classes', np.empty(0)))

        valid_detections = scores > self.conf_threshold
        boxes = boxes[valid_detections]
        scores = scores[valid_detections]
        class_ids = class_ids[valid_detections]

        if len(boxes) == 0:
            return [], [], [], image

        # Scale boxes back to original image size
        boxes[:, [0, 2]] *= (self.img_width / self.input_width)
        boxes[:, [1, 3]] *= (self.img_height / self.input_height)
        boxes = boxes.astype(int)
        
        drawn_image = image.copy()
        for box, score, cls_id in zip(boxes, scores, class_ids):
            x1, y1, x2, y2 = box
            color = (0, 255, 0)  # Green color for boxes
            label = f"red_tape {score:.2f}"

            cv2.rectangle(drawn_image, (x1, y1), (x2, y2), color, 2)
            cv2.putText(drawn_image, label, (x1, y1 - 10),
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

        return boxes, scores, class_ids, drawn_image

def process_images():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    image_extensions = ('.jpg', '.jpeg', '.png', '.bmp')
    image_files = [f for f in os.listdir(INPUT_FOLDER) 
                  if f.lower().endswith(image_extensions)]
    
    if not image_files:
        print(f"No images found in {INPUT_FOLDER}")
        return
    
    print(f"Found {len(image_files)} images to process")
    
    client = TritonDetectionClient(TRITON_SERVER_URL, MODEL_NAME)
    
    for filename in tqdm(image_files, desc="Processing images"):
        try:
            image_path = os.path.join(INPUT_FOLDER, filename)
            image = cv2.imread(image_path)
            
            if image is None:
                print(f"Could not read image: {filename}")
                continue
            
            boxes, scores, class_ids, annotated_img = client.infer(image)
            output_path = os.path.join(OUTPUT_FOLDER, filename)
            cv2.imwrite(output_path, annotated_img)
            
            if len(boxes) > 0:
                print(f"\nDetections in {filename}: {len(boxes)}")
                
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

if __name__ == '__main__':
    process_images()
    print("Processing complete!")
