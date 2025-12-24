import asyncio
import json
import time
import nats
import requests
from typing import Dict, List, Optional

with open('plc_tags.json') as f:
    PLC_TAGS = json.load(f)

class RestPLC:
    def __init__(self, ip: str):
        self.ip = ip
        self.base_url = f"http://{ip}:39320/iotgateway"
        
    def write(self, tag: str, value) -> bool:
        url = f"{self.base_url}/write"
        data = [{"id": tag, "v": value}]
        headers = {"Content-Type": "application/json"}
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            if response.status_code == 200:
                print(f"Successfully wrote {value} to {tag} on PLC {self.ip}")
                return True
            else:
                print(f"Write failed on {self.ip}: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Error writing to {self.ip}: {str(e)}")
            return False

class Server:
    def __init__(self):
        self.plcs = {
            "25": RestPLC('192.168.1.154'),  # MC25
            "26": RestPLC('192.168.1.154'),  # MC26 (same IP as MC25)
            "27": RestPLC('192.168.1.153'),  # MC27
            "30": RestPLC('192.168.1.153'),  # MC30 (same IP as MC27)
            "28": RestPLC('192.168.1.152'),  # MC28
            "29": RestPLC('192.168.1.152')   # MC29 (same IP as MC28)
        }
        
        self.plc_topics = {
            "25": "adv.154",  # MC25
            "26": "adv.154",  # MC26 (sharing with MC25)
            "27": "adv.153",  # MC27
            "30": "adv.153",  # MC30 (sharing with MC27)
            "28": "adv.152",  # MC28
            "29": "adv.152"   # MC29 (sharing with MC28)
        }
        
    def get_tag_info(self, plc_id: str, name: str) -> Optional[Dict]:
        if plc_id in ["25", "26", "27", "28", "29", "30"]:
            for item in PLC_TAGS.get("MC25", []):
                if item["name"].lower() == name.lower():
                    return item
        return None

    async def message_handler(self, msg):
        try:
            req = json.loads(msg.data)
            print("Request JSON:", req)
            plc_id = req.get("plc")
            plc = self.plcs.get(plc_id)
            
            if not plc:
                print(f"Invalid PLC ID: {plc_id}")
                return await msg.respond(json.dumps({"error": "Invalid PLC ID"}).encode())
            
            name = req.get("name")
            if not name:
                print("Missing name in request")
                return await msg.respond(json.dumps({"error": "Missing name"}).encode())
                
            command = req.get("command")
            if not command or command.upper() not in ["UPDATE", "TOGGLE"]:
                print("Invalid or missing command")
                return await msg.respond(json.dumps({"error": "Invalid command. Use UPDATE or TOGGLE"}).encode())
                
            tag_info = self.get_tag_info(plc_id, name)
            if not tag_info:
                print(f"Tag not found for name: {name}")
                return await msg.respond(json.dumps({"error": "Tag not found"}).encode())
                
            if tag_info["enable"] != 1:
                print(f"Tag {name} is not enabled for writing")
                return await msg.respond(json.dumps({"error": "Tag not enabled for writing"}).encode())
            
            if command.upper() == "TOGGLE":
                if name.lower() not in ["start", "stop", "reset"]:
                    print("TOGGLE command can only be used with start/stop/reset")
                    return await msg.respond(json.dumps({
                        "error": "TOGGLE command can only be used with start/stop/reset names"
                    }).encode())
                
                if not tag_info.get("tag"):
                    print(f"No tag defined for {name}")
                    return await msg.respond(json.dumps({
                        "error": f"No tag defined for {name}"
                    }).encode())
                
                # Send True pulse for 1.5 seconds
                plc.write(tag_info["tag"], True)
                print(f"Wrote True to {tag_info['tag']} on PLC {plc_id}")
                
                # Create a task to handle the pulse timeout without blocking
                async def pulse_off():
                    await asyncio.sleep(1.5)  # 1.5 second delay
                    plc.write(tag_info["tag"], False)
                    print(f"Wrote False to {tag_info['tag']} on PLC {plc_id}")
                
                asyncio.create_task(pulse_off())
                
                return await msg.respond(json.dumps({
                    "plc": plc_id,
                    "ack": True,
                    "message": f"Started 1.5s pulse for {name}"
                }).encode())
            
            elif command.upper() == "UPDATE":
                value = req.get("value")
                if value is None:
                    print("Missing value for UPDATE command")
                    return await msg.respond(json.dumps({"error": "Missing value for UPDATE"}).encode())
                
                try:
                    value = int(value) * 10  # Convert and scale the value
                except ValueError:
                    print(f"Invalid value format: {value}")
                    return await msg.respond(json.dumps({
                        "error": "Value must be a number"
                    }).encode())
                
                if not tag_info.get("tag"):
                    print(f"No tag defined for {name}")
                    return await msg.respond(json.dumps({
                        "error": f"No tag defined for {name}"
                    }).encode())
                
                write_tag = tag_info["tag"]
                
                success = plc.write(write_tag, value)
                if success:
                    return await msg.respond(json.dumps({
                        "plc": plc_id,
                        "ack": True,
                        "message": f"Updated {name} to {value}"
                    }).encode())
                else:
                    return await msg.respond(json.dumps({
                        "error": f"Failed to update {name}"
                    }).encode())
                
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            await msg.respond(json.dumps({"error": "Invalid JSON format"}).encode())
        except Exception as e:
            print(f"Error processing request: {e}")
            await msg.respond(json.dumps({"error": str(e)}).encode())

    async def run(self):
        try:
            nc = await nats.connect("nats://192.168.1.149:4222")
            subscriptions = []
            for topic in set(self.plc_topics.values()):
                sub = await nc.subscribe(topic, cb=self.message_handler)
                subscriptions.append(sub)
                print(f"Subscribed to NATS topic: {topic}")
            
            print("Server listening for PLC commands...")
            await asyncio.Event().wait()
            
        except Exception as e:
            print(f"NATS connection error: {e}")
        finally:
            for sub in subscriptions:
                await sub.unsubscribe()

if __name__ == "__main__":
    try:
        asyncio.run(Server().run())
    except KeyboardInterrupt:
        print("Server shutting down...")
    except Exception as e:
        print(f"Unexpected error: {e}")
