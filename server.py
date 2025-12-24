import asyncio
import nats
import json
import os
from dataclasses import asdict, dataclass


@dataclass
class Payload:
    plc: str
    tag: str
    value:float
    status:bool

@dataclass
class Reponse:
    plc:str
    value:float
    status:bool
    ack:bool

@dataclass
class request:
    plc:str
    tag:str
    value:float

async def run():
    # Connect to the NATS server
    nc = await nats.connect("nats://192.168.0.123:4222")

    # Define the request handler
    async def message_handler(msg):
        print(f"Received request: {msg.data.decode()}")
        response = f"Hello, {msg.data.decode()}"
        payload = Response(plc="22", value=27.0,status=False,ack=True)
        bytes_ = json.dumps(asdict(payload)).encode()
        await msg.respond(bytes_)

    # Subscribe to a subject and handle requests
    await nc.subscribe("greeting", cb=message_handler)

    print("Server is listening for requests on 'greeting'...")
    
    # Keep the server running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(run())
