import requests
import json

# Function for POST request
def post_request():
    url = "http://192.168.1.153:39321/iotgateway/write"
    data = [{"id": "loop4.mc25.hor_sealer_front_sp", "v": 1400}]
    headers = {"Content-Type": "application/json"}
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    # Print response
    print(f"POST Response Status Code: {response.status_code}")
    print(f"POST Response Body: {response.text}")
    
# Function for GET request to browse
def get_browse_request():
    print("get browse request")
    url = "http://192.168.1.154:39320/iotgateway/browse"
    headers = {"Content-Type": "application/json"}
    
    response = requests.get(url, headers=headers)
    
    # Print response
    print(f"GET Browse Response Status Code: {response.status_code}")
    print(f"GET Browse Response Body: {response.text}")

# Function for GET request to read with specific IDs
def get_read_request():
    print("read request")
    url = "http://192.168.1.153:39321/iotgateway/read"
    params = {"ids": "loop4.mc25.hor_sealer_front_sp"}  # Query parameter
    headers = {"Content-Type": "application/json"}
    
    response = requests.get(url, headers=headers, params=params)
    
    # Print response
    print(f"GET Read Response Status Code: {response.status_code}")
    print(f"GET Read Response Body: {response.text}")

# Run all requests
#post_request()
get_browse_request()
get_read_request()

