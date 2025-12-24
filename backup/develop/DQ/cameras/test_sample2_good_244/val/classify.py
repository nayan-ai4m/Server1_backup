import logging
import os
import time
import numpy as np
import cv2
import tritonclient.grpc as grpcclient
from datetime import datetime


def transform_image(image):
    """
    Load and transform an image using PIL, OpenCV, and NumPy.

    Args:
        image_path (str): Path to the input image

    Returns:
        numpy.ndarray: Transformed and normalized image as a numpy array
    """
    try:
        # Load image using PIL and convert to RGB

        # Resize image using OpenCV
        #image = cv2.resize(image, (224, 224))

        # Convert to float32 and normalize to [0, 1]
        image = image.astype(np.float32) / 255.0

        # Define mean and std for normalization (ImageNet values)
        mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
        std = np.array([0.229, 0.224, 0.225], dtype=np.float32)

        # Normalize image: (image - mean) / std
        image = (image - mean) / std

        # Transpose to (channels, height, width)
        image = np.transpose(image, (2, 0, 1))

        # Add batch dimension
        input_data = np.expand_dims(image, axis=0)

        return input_data
    except Exception as e:
        print(e)


def setup_logging():
    todays_date = datetime.now().strftime("%Y-%m-%d")
    log_directory = os.path.join("logs", todays_date)
    os.makedirs(log_directory, exist_ok=True)
    log_filename = os.path.join(log_directory, f"{todays_date}_truck_type.log")

    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


setup_logging()



try:
    print("started")
    triton_client = grpcclient.InferenceServerClient("localhost:9002")


    classes = [
        "bad","good"
    ]


    classify_inputs = []
    classify_outputs = []

    classify_inputs.append(grpcclient.InferInput("input", [1, 3, 224, 224], "FP32"))


    image_dir = os.listdir('test_mc17_may28/')
    print(image_dir)
    for image in sorted(image_dir):
        img = cv2.imread(os.path.join('test_mc17_may28',image))
        

        cropped_image = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        #transformed_image = transform(pil_image)
        #input_data = np.array(transformed_image, dtype=np.float32)
        #input_data = np.expand_dims(input_data, axis=0)
        input_data = transform_image(cropped_image)
        classify_inputs[0].set_data_from_numpy(input_data)
        result = triton_client.infer(
            model_name="perforation_classify",
            inputs=classify_inputs,
            outputs=classify_outputs,
        )
        predictions = np.squeeze(result.as_numpy("output"))
        results = predictions.argmax()
        print(image,classes[results],results)


except Exception as e:
    print(e)

