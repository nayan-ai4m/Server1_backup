import requests
import json

# Define the URL
url = "http://192.168.1.154:39320/iotgateway/write"

# Define the payload (data to be sent)
data = [{"id": "loop4.mc25.hor_sealer_front_sp", "v": 1400}]

# Define the headers
headers = {
    "Content-Type": "application/json"
}

# Send the POST request
response = requests.post(url, headers=headers, data=json.dumps(data))

# Print the response from the server
print(response.status_code)  # Status code
print(response.text)  # Response body

