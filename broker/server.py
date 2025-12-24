import asyncio
import nats
import json
import os
from dataclasses import asdict, dataclass



@dataclass
class Response:
    plc: str
    command:str
    ack:bool

@dataclass
class Request:
    plc:str
    tag:str
    value:float
    status:bool
    command:str

async def run():
    # Connect to the NATS server
    nc = await nats.connect("nats://0.0.0.0:4222")

    # Define the request handler
    async def message_handler(msg):
        request = Request(**json.loads(msg.data))
        print(request)
        payload = Response(plc="22", command="TOGGLE",ack=True)
        bytes_ = json.dumps(asdict(payload)).encode()
        await msg.respond(bytes_)

    # Subscribe to a subject and handle requests
    await nc.subscribe("plc.17", cb=message_handler)

    print("Server is listening for requests on 'greeting'...")
    
    # Keep the server running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(run())
