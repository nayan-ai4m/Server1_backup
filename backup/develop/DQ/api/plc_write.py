import requests
import json
import time
import asyncio
import nats

ALLOWED_TAGS = ['loop4.mc25.hor_sealer_front_sp', 'loop4.mc25.hor_sealer_rear_sp', 'loop4.mc25.hor_stroke_length1','loop4.mc25.hor_stroke_length2', 'loop4.mc25.piston_stroke_length_left', 'loop4.mc25.piston_stroke_length_right','loop4.mc25.ver_sealer_front_sp_1', 'loop4.mc25.ver_sealer_front_sp_10', 'loop4.mc25.ver_sealer_front_sp_11','loop4.mc25.ver_sealer_front_sp_12', 'loop4.mc25.ver_sealer_front_sp_13', 'loop4.mc25.ver_sealer_front_sp_2','loop4.mc25.ver_sealer_front_sp_3', 'loop4.mc25.ver_sealer_front_sp_4', 'loop4.mc25.ver_sealer_front_sp_5','loop4.mc25.ver_sealer_front_sp_6', 'loop4.mc25.ver_sealer_front_sp_7', 'loop4.mc25.ver_sealer_front_sp_8','loop4.mc25.ver_sealer_rear_sp_1', 'loop4.mc25.ver_sealer_rear_sp_10', 'loop4.mc25.ver_sealer_rear_sp_11','loop4.mc25.ver_sealer_rear_sp_12', 'loop4.mc25.ver_sealer_rear_sp_13', 'loop4.mc25.ver_sealer_rear_sp_2','loop4.mc25.ver_sealer_rear_sp_3', 'loop4.mc25.ver_sealer_rear_sp_4', 'loop4.mc25.ver_sealer_rear_sp_5','loop4.mc25.ver_sealer_rear_sp_6', 'loop4.mc25.ver_sealer_rear_sp_7', 'loop4.mc25.ver_sealer_rear_sp_8','loop4.mc25.ver_sealer_rear_sp_9', 'loop4.mc26.hor_sealer_front_sp', 'loop4.mc26.hor_sealer_rear_sp','loop4.mc26.hor_stroke_length1', 'loop4.mc26.hor_stroke_length2', 'loop4.mc26.piston_stroke_length_left','loop4.mc26.piston_stroke_length_right', 'loop4.mc26.ver_sealer_front_sp_1', 'loop4.mc26.ver_sealer_front_sp_10','loop4.mc26.ver_sealer_front_sp_11', 'loop4.mc26.ver_sealer_front_sp_12', 'loop4.mc26.ver_sealer_front_sp_13','loop4.mc26.ver_sealer_front_sp_2', 'loop4.mc26.ver_sealer_front_sp_3', 'loop4.mc26.ver_sealer_front_sp_4','loop4.mc26.ver_sealer_front_sp_5', 'loop4.mc26.ver_sealer_front_sp_6', 'loop4.mc26.ver_sealer_front_sp_7','loop4.mc26.ver_sealer_front_sp_8', 'loop4.mc26.ver_sealer_front_sp_9', 'loop4.mc26.ver_sealer_rear_sp_1','loop4.mc26.ver_sealer_rear_sp_10', 'loop4.mc26.ver_sealer_rear_sp_11', 'loop4.mc26.ver_sealer_rear_sp_12','loop4.mc26.ver_sealer_rear_sp_13', 'loop4.mc26.ver_sealer_rear_sp_2', 'loop4.mc26.ver_sealer_rear_sp_3','loop4.mc26.ver_sealer_rear_sp_4', 'loop4.mc26.ver_sealer_rear_sp_5', 'loop4.mc26.ver_sealer_rear_sp_6','loop4.mc26.ver_sealer_rear_sp_7', 'loop4.mc26.ver_sealer_rear_sp_8', 'loop4.mc26.ver_sealer_rear_sp_9','loop4.mc26.ver_stroke_length1', 'loop4.mc26.ver_stroke_length2']

class PLCAPI:
    def __init__(self, base_url="http://192.168.1.154:39320/iotgateway"):
        self.base_url = base_url
        self.headers = {"Content-Type": "application/json"}

    def validate_tag(self, tag):
        return tag in ALLOWED_TAGS

    def write(self, tag, value):
        if not self.validate_tag(tag):
            print(f"[INVALID TAG] Write aborted. '{tag}' is not in allowed tag list.")
            return False
        
        data = [{"id": tag, "v": value}]
        try:
            response = requests.post(f"{self.base_url}/write", headers=self.headers, data=json.dumps(data))
            print(f"[WRITE] Tag: {tag}, Value: {value}, Status: {response.status_code}")
            print(f"[WRITE RESPONSE] {response.text}")
            return response.status_code == 200
        except requests.RequestException as e:
            print(f"[WRITE ERROR] Failed to write to {tag}: {e}")
            return False

    def toggle(self, tag, delay=2):
        print(f"[TOGGLE] Tag: {tag} - Toggling True/False with {delay}s delay")
        if self.write(tag, True):
            time.sleep(delay)
            self.write(tag, False)

    def browse(self):
        print("[BROWSE] Sending browse request...")
        try:
            response = requests.get(f"{self.base_url}/browse", headers=self.headers)
            print(f"[BROWSE RESPONSE] Status: {response.status_code}\n{response.text}")
        except requests.RequestException as e:
            print(f"[BROWSE ERROR] {e}")

    def read(self, tag):
        print(f"[READ] Sending read request for tag: {tag}")
        try:
            response = requests.get(f"{self.base_url}/read", headers=self.headers, params={"ids": tag})
            print(f"[READ RESPONSE] Status: {response.status_code}\n{response.text}")
        except requests.RequestException as e:
            print(f"[READ ERROR] {e}")

class Server:
    def __init__(self):
        self.api = PLCAPI()

    async def message_handler(self, msg):
        try:
            req = json.loads(msg.data)
            print("[REQUEST RECEIVED]", req)

            tag = req.get("tag")
            tag = tag.lower()
            plc = req.get("plc")
            actual_tag=f'loop4.mc{plc}.{tag}'
            value = req.get("value")
            command = req.get("command")

            if not actual_tag:
                await msg.respond(json.dumps({"error": "Missing 'tag' in request"}).encode())
                return

            if not self.api.validate_tag(actual_tag):
                print(f"[INVALID TAG] {actual_tag} not allowed.")
                await msg.respond(json.dumps({"error": f"Invalid tag: {actual_tag}"}).encode())
                return

            if command == "TOGGLE":
                self.api.toggle(actual_tag)
                await msg.respond(json.dumps({"tag": actual_tag, "ack": True, "command": "TOGGLE"}).encode())

            elif command in ["UPDATE_TEMP", "WRITE"]:
                # If it's "UPDATE_TEMP", you might need to modify tag like previous code
                final_tag = f"{actual_tag}.SetValue" if command == "UPDATE_TEMP" else actual_tag
                success = self.api.write(final_tag, value)
                await msg.respond(json.dumps({"tag": final_tag, "ack": success}).encode())

            else:
                await msg.respond(json.dumps({"error": f"Unsupported command: {command}"}).encode())

        except json.JSONDecodeError as e:
            print(f"[JSON ERROR] {e}")
            await msg.respond(json.dumps({"error": "Invalid JSON format"}).encode())
        except Exception as e:
            print(f"[MESSAGE HANDLER ERROR] {e}")
            await msg.respond(json.dumps({"error": str(e)}).encode())

    async def run(self):
        try:
            nc = await nats.connect("nats://192.168.1.149:4222")
            await nc.subscribe("plc.154", cb=self.message_handler)
            print("[SERVER] Listening for messages on 'plc.154'...")
            await asyncio.Event().wait()
        except Exception as e:
            print(f"[NATS ERROR] {e}")

if __name__ == "__main__":
    try:
        asyncio.run(Server().run())
    except KeyboardInterrupt:
        print("[SHUTDOWN] Server shutting down...")
    except Exception as e:
        print(f"[UNEXPECTED ERROR] {e}")

