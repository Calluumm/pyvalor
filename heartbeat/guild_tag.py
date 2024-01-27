import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
import time
import datetime
import sys
from log import logger

class GuildTagTask(Task):
    def __init__(self, sleep):
        super().__init__(sleep)
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def guild_tag_task():
            while not self.finished:
                # this entire routine will take like 10MB/beat
                logger.info("GUILD TAG NAME TASK START")
                start = time.time()

                updated_guilds = set([x for x in await Async.get("https://api.wynncraft.com/v3/guild/list/guild")]) # like 200K mem max
                res = Connection.execute("SELECT guild FROM guild_tag_name")
                current_guilds = set(x[0] for x in res)
                current_guilds_lower = {x.lower().strip() for x in current_guilds}
                
                difference_dupe = updated_guilds-current_guilds
                difference = {x for x in difference_dupe if not x.lower().strip() in current_guilds_lower}

                logger.info(f"GUILD TAG NAME TASK: (difference: {len(difference)})")

                inserts = []
                logger.info(f"{difference}")

                for new_guild in difference:
                    req = await Async.get("https://api.wynncraft.com/v3/guild/"+new_guild)
                    tag = req["prefix"]
                    
                    current_guild_members = set()
                    for rank in req["members"]:
                        if type(req["members"][rank]) != dict: continue
                        current_guild_members |= req["members"][rank].keys()

                    n_members = len(current_guild_members)
                    inserts.append(f"('{new_guild}','{tag}',{n_members})")
                    await asyncio.sleep(0.3)

                # batch insert if the # is too long for some reason
                for i in range(0, len(inserts), 50):
                    batch = inserts[i:min(i+50, len(inserts))]
                    Connection.execute("REPLACE INTO guild_tag_name VALUES "+','.join(batch))

                end = time.time()
                logger.info("GUILD TAG NAME TASK"+f" {end-start}s")
                
                await asyncio.sleep(self.sleep)
        
            logger.info("GuildTagTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(guild_tag_task))