import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import time
import cv2

class Stream:
    def __init__(self, name, size=(640, 480), quality=80, fps=20):
        self.name = name
        self.size = size
        self.quality = quality
        self.fps = fps
        self.frame = None
        self.lock = threading.Lock()

    def set_frame(self, frame):
        with self.lock:
            self.frame = cv2.resize(frame, self.size)

    def get_jpeg(self):
        with self.lock:
            if self.frame is None:
                return None
            ret, jpeg = cv2.imencode('.jpg', self.frame, [int(cv2.IMWRITE_JPEG_QUALITY), self.quality])
            return jpeg.tobytes() if ret else None

class MJPEGRequestHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()

    def do_GET(self):
        stream = self.server.streams.get(self.path.strip("/"))
        if not stream:
            self.send_error(404)
            return

        self.send_response(200)
        self.send_header('Content-type', 'multipart/x-mixed-replace; boundary=frame')
        self.send_header('Access-Control-Allow-Origin', '*')  # 🔥 CORS Header
        self.end_headers()

        try:
            while True:
                jpeg = stream.get_jpeg()
                if jpeg:
                    self.wfile.write(b"--frame\r\n")
                    self.wfile.write(b"Content-Type: image/jpeg\r\n\r\n" + jpeg + b"\r\n")
                time.sleep(1.0 / stream.fps)
        except (BrokenPipeError, ConnectionResetError):
            pass  # client disconnected

    def log_message(self, format, *args):
        return  # silence default logging

class MjpegServer:
    def __init__(self, host='0.0.0.0', port=8080):
        self.server = HTTPServer((host, port), MJPEGRequestHandler)
        self.server.streams = {}
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True

    def add_stream(self, stream):
        self.server.streams[stream.name] = stream

    def start(self):
        print("Starting MJPEG server...")
        self.thread.start()

    def stop(self):
        print("Stopping MJPEG server...")
        self.server.shutdown()
        self.thread.join()
