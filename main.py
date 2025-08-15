import asyncio
from heartbeat import Heartbeat
import websockets
from rpc import player_stats_updater_service
import os
import dotenv

dotenv.load_dotenv()

ev = asyncio.get_event_loop()

async def terr_connect(websocket, path):
    Heartbeat.wsconns.add(websocket)
    try:
        await websocket.recv()
    finally:
        Heartbeat.wsconns.remove(websocket)

start = websockets.serve(terr_connect, "0.0.0.0", 8080)
ev.run_until_complete(start)
ev.create_task(player_stats_updater_service.serve())

Heartbeat.run_tasks()

ev.run_forever()