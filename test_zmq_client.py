import zmq
import cv2
import numpy as np

def main():
    # Initialize ZeroMQ context and socket
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all messages

    print("Connected to publisher at tcp://localhost:5555")

    try:
        while True:
            # Receive the encoded image data
            message = socket.recv()
            
            # Convert the received bytes to a NumPy array
            encoded_data = np.frombuffer(message, dtype=np.uint8)
            
            # Decode the JPEG data into an image
            image = cv2.imdecode(encoded_data, cv2.IMREAD_COLOR)
            
            if image is not None:
                # Print image dimensions
                height, width = image.shape[:2]
                print(f"Received image: {width}x{height}")
                
                # Display the image (optional)
            else:
                print("Failed to decode image")

    except KeyboardInterrupt:
        print("Subscriber stopped by user")
    finally:
        # Clean up
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
