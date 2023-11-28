import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
from collections import defaultdict
import time
import datetime
from log import logger

class PlayerActivityTask(Task):
    def __init__(self, sleep):
        super().__init__(sleep)
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def player_activity_task():
            logger.info("PLAYER ACTIVITY TRACK START")
            start = time.time()
            online_all = await Async.get("https://api.wynncraft.com/v3/player")
            online_all = {x for x in online_all["players"]}

            inserts = []

            res = Connection.execute('''SELECT uuid_name.name, player_stats.guild, player_stats.uuid FROM guild_list
LEFT JOIN player_stats ON guild_list.guild=player_stats.guild
LEFT JOIN uuid_name ON uuid_name.uuid=player_stats.uuid;''')
            
            player_to_guild = {name: (guild, uuid) for name, guild, uuid in res}
            intersection = online_all & player_to_guild.keys()

            for player_name in intersection:
                guild, uuid = player_to_guild[player_name]
                if not player_name or not guild or not uuid: continue
                inserts.append(f"(\"{player_name}\", \"{guild}\", {int(time.time())}, \"{uuid}\")")

            for i in range(0, 32, len(inserts)):
                try:
                    Connection.execute(f"INSERT INTO activity_members VALUES {','.join(inserts[i:i+32])}")
                except Exception as e:
                    logger.info(f"PLAYER ACTIVITY TASK ERROR")
                    logger.exception(e)
                    logger.warn(f"insertion looks like: {','.join(inserts[i:i+32])}")
                    self.finished = True

            end = time.time()
            logger.info("PLAYER ACTIVITY TASK"+f" {end-start}s")
            
            await asyncio.sleep(self.sleep)

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(player_activity_task))
        