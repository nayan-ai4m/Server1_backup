from dataclasses import dataclass
from typing import Tuple
import os
import cv2, glob
import numpy as np
import random
import math
import tritonclient.grpc as grpcclient
import time
import zmq
import datetime
import uuid
import psycopg2

random.seed(42)

@dataclass
class Tensor:
    name: str
    dtype: np.dtype
    shape: Tuple
    cpu: np.ndarray
    gpu: int

CLASSES_DET = ('leakage')

COLORS = {
    cls: [random.randint(0, 255) for _ in range(3)]
    for i, cls in enumerate(CLASSES_DET)
}


class TritonClient:
    def __init__(self, url: str, model_name: str, model_version: str = '1') -> None:
        self.client = grpcclient.InferenceServerClient(url=url)
        self.model_name = model_name
        self.model_version = model_version
        self.num_masks = 32
        self.input_height = 1280
        self.input_width = 1280
        self.mask_alpha = 0.5
        self.conf_threshold = 0.3
        self.iou_threshold = 0.5

    def sigmoid(self, x: np.ndarray) -> np.ndarray:
        return 1. / (1. + np.exp(-x))

    def process_box_output(self, box_output):
        predictions = np.squeeze(box_output).T
        num_classes = box_output.shape[1] - self.num_masks - 4
        scores = np.max(predictions[:, 4:4 + num_classes], axis=1)
        predictions = predictions[scores > self.conf_threshold, :]
        scores = scores[scores > self.conf_threshold]
        if len(scores) == 0:
            return [], [], [], np.array([])
        box_predictions = predictions[..., :num_classes + 4]
        mask_predictions = predictions[..., num_classes + 4:]
        class_ids = np.argmax(box_predictions[:, 4:], axis=1)
        boxes = self.extract_boxes(box_predictions)
        indices = self.nms(boxes, scores, self.iou_threshold)

        return boxes[indices], scores[indices], class_ids[indices], mask_predictions[indices]

    def process_mask_output(self, mask_predictions, mask_output):
        if mask_predictions.shape[0] == 0:
            return []

        mask_output = np.squeeze(mask_output)
        num_mask, mask_height, mask_width = mask_output.shape
        masks = self.sigmoid(mask_predictions @ mask_output.reshape((num_mask, -1)))
        masks = masks.reshape((-1, mask_height, mask_width))
        scale_boxes = self.rescale_boxes(self.boxes, (self.img_height, self.img_width), (mask_height, mask_width))
        mask_maps = np.zeros((len(scale_boxes), self.img_height, self.img_width))
        blur_size = (int(self.img_width / mask_width), int(self.img_height / mask_height))
        for i in range(len(scale_boxes)):
            scale_x1 = int(math.floor(scale_boxes[i][0]))
            scale_y1 = int(math.floor(scale_boxes[i][1]))
            scale_x2 = int(math.ceil(scale_boxes[i][2]))
            scale_y2 = int(math.ceil(scale_boxes[i][3]))

            x1 = int(math.floor(self.boxes[i][0]))
            y1 = int(math.floor(self.boxes[i][1]))
            x2 = int(math.ceil(self.boxes[i][2]))
            y2 = int(math.ceil(self.boxes[i][3]))

            scale_crop_mask = masks[i][scale_y1:scale_y2, scale_x1:scale_x2]
            crop_mask = cv2.resize(scale_crop_mask, (x2 - x1, y2 - y1), interpolation=cv2.INTER_CUBIC)
            crop_mask = cv2.blur(crop_mask, blur_size)
            crop_mask = (crop_mask > 0.5).astype(np.uint8)
            mask_maps[i, y1:y2, x1:x2] = crop_mask

        return mask_maps

    def extract_boxes(self, box_predictions):
        boxes = box_predictions[:, :4]
        boxes = self.rescale_boxes(boxes, (self.input_height, self.input_width), (self.img_height, self.img_width))
        boxes = self.xywh2xyxy(boxes)
        boxes[:, 0] = np.clip(boxes[:, 0], 0, self.img_width)
        boxes[:, 1] = np.clip(boxes[:, 1], 0, self.img_height)
        boxes[:, 2] = np.clip(boxes[:, 2], 0, self.img_width)
        boxes[:, 3] = np.clip(boxes[:, 3], 0, self.img_height)

        return boxes

    @staticmethod
    def rescale_boxes(boxes, input_shape, image_shape):
        input_shape = np.array([input_shape[1], input_shape[0], input_shape[1], input_shape[0]])
        boxes = np.divide(boxes, input_shape, dtype=np.float32)
        boxes *= np.array([image_shape[1], image_shape[0], image_shape[1], image_shape[0]])

        return boxes

    def nms(self, boxes, scores, iou_threshold):
        sorted_indices = np.argsort(scores)[::-1]
        keep_boxes = []
        while sorted_indices.size > 0:
            box_id = sorted_indices[0]
            keep_boxes.append(box_id)
            ious = self.compute_iou(boxes[box_id, :], boxes[sorted_indices[1:], :])
            keep_indices = np.where(ious < iou_threshold)[0]
            sorted_indices = sorted_indices[keep_indices + 1]

        return keep_boxes

    def compute_iou(self, box, boxes):
        xmin = np.maximum(box[0], boxes[:, 0])
        ymin = np.maximum(box[1], boxes[:, 1])
        xmax = np.minimum(box[2], boxes[:, 2])
        ymax = np.minimum(box[3], boxes[:, 3])
        intersection_area = np.maximum(0, xmax - xmin) * np.maximum(0, ymax - ymin)
        box_area = (box[2] - box[0]) * (box[3] - box[1])
        boxes_area = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
        union_area = box_area + boxes_area - intersection_area
        iou = intersection_area / union_area

        return iou

    def xywh2xyxy(self, x):
        y = np.copy(x)
        y[..., 0] = x[..., 0] - x[..., 2] / 2
        y[..., 1] = x[..., 1] - x[..., 3] / 2
        y[..., 2] = x[..., 0] + x[..., 2] / 2
        y[..., 3] = x[..., 1] + x[..., 3] / 2
        return y

    def prepare_input(self, image):
        self.img_height, self.img_width = image.shape[:2]
        input_img = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        input_img = cv2.resize(input_img, (self.input_width, self.input_height))
        input_img = input_img / 255.0
        input_img = input_img.transpose(2, 0, 1)
        input_tensor = input_img[np.newaxis, :, :, :].astype(np.float32)
        return input_tensor

    def draw_masks(self, image, boxes, class_ids, mask_alpha=0.3, mask_maps=None):
        mask_img = np.zeros((1944,2592, 3), dtype=np.uint8)
        for i, (box, class_id) in enumerate(zip(boxes, class_ids)):
            color = COLORS[CLASSES_DET[class_id]]
            x1, y1, x2, y2 = box.astype(int)
            if mask_maps is None:
                cv2.rectangle(mask_img, (x1, y1), (x2, y2), color, -1)
            else:
                crop_mask = mask_maps[i][y1:y2, x1:x2, np.newaxis]
                crop_mask_img = mask_img[y1:y2, x1:x2]
                crop_mask_img = crop_mask_img * (1 - crop_mask) + crop_mask * 255
                mask_img[y1:y2, x1:x2] = crop_mask_img
        return mask_img

    def draw(self, image, boxes, scores, class_ids, mask_alpha=0.3, mask_maps=None):
        raw_image = image
        mask_img = self.draw_masks(image, boxes, class_ids, mask_alpha, mask_maps)
        for box, score, class_id in zip(boxes, scores, class_ids):
            color = COLORS[CLASSES_DET[class_id]]
            x1, y1, x2, y2 = box.astype(int)
            cv2.rectangle(raw_image, (x1, y1), (x2, y2), color, 2)
            caption = f'{CLASSES_DET[class_id]} {int(score * 100)}%'
            cv2.putText(raw_image, caption, (x1, y1), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2, cv2.LINE_AA)
        return raw_image

    def infer(self, image):
        input_tensor = self.prepare_input(image)
        inputs = [grpcclient.InferInput('images', input_tensor.shape, "FP32")]
        inputs[0].set_data_from_numpy(input_tensor)
        outputs = [grpcclient.InferRequestedOutput('output0'),
                   grpcclient.InferRequestedOutput('output1')]

        results = self.client.infer(model_name=self.model_name, model_version=self.model_version, inputs=inputs, outputs=outputs)
        output_0 = results.as_numpy('output1')
        output_2 = results.as_numpy('output0')
        self.boxes, self.scores, self.class_ids, mask_pred = self.process_box_output(output_2)
        self.mask_maps = self.process_mask_output(mask_pred, output_0)
        combined_img = self.draw(image, self.boxes, self.scores, self.class_ids, self.mask_alpha, mask_maps=self.mask_maps)
        #if len(self.boxes) > 0:
        #    print("boxes:",self.boxes,"classes:",self.class_ids)
        #    return self.boxes, self.class_ids, combined_img, self.scores
        return self.boxes, self.class_ids, combined_img, self.scores

insert_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s,%s,%s);"""
db_connection = """postgres://postgres:ai4m2024@localhost:5432/hul?sslmode=disable"""
connection = psycopg2.connect(db_connection)


def insert_row(data):
    try:
        cur = connection.cursor()
        cur.execute(insert_query,(data))
        connection.commit()
    except Exception as e : 
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        connection.rollback()
        print(e)

infer_path = "./inferred_frames/"
client = TritonClient('localhost:8006', 'leakage_seg')

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://localhost:5555")
socket.setsockopt_string(zmq.SUBSCRIBE, "")

try:
    while True:
        frame = socket.recv()
        frame = cv2.imdecode(np.frombuffer(frame, dtype=np.uint8), cv2.IMREAD_COLOR)
        print("new frame : ",frame.shape)
    
        if frame is not None:
            result = client.infer(frame)
            filename = f"infered_{int(time.time())}.png"
            if result is not None:
                cv2.imwrite(os.path.join(infer_path, filename), result[2])
                data = (datetime.datetime.now(), uuid.uuid4(), "baumer", "MC18", str(filename), "leakage", "quality" )
                #insert_row(data)

except KeyboardInterrupt:
    print("Subscriber stopped by user")
finally:
    socket.close()
    context.term()

