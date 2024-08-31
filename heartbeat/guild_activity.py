import asyncio
import aiohttp
from db import Connection
from network import Async
from dotenv import load_dotenv
from .task import Task
import time
import datetime
import os
from log import logger

load_dotenv()
webhook = os.environ["JOINLEAVE"]

class GuildActivityTask(Task):
    def __init__(self, start_after, sleep, wsconns):
        super().__init__(start_after, sleep)
        self.wsconns = wsconns
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def guild_activity_task():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("GUILD ACTIVITY TRACK START")
                start = time.time()

                guild_data_members = (await Async.get("https://api.wynncraft.com/v3/guild/Titans%20Valor"))["members"]
                current_guild_members = set()
                for rank in guild_data_members:
                    if type(guild_data_members[rank]) != dict: continue
                    current_guild_members |= guild_data_members[rank].keys()

                old_guild_members = {x[1] for x in Connection.execute(f"SELECT * FROM guild_member_cache") if x[0] == "Titans Valor"}
                left = [f'"{x}"' for x in old_guild_members-current_guild_members]
                join = [f'"{x}"' for x in current_guild_members-old_guild_members]
                
                if left or join:
                    for ws in self.wsconns:
                        await ws.send('{"type":"join","leave":'+f'[{",".join(left)}],"join":'+f'[{",".join(join)}]' + "}")
                    await Async.post(webhook, {"content": f"Joined: {repr(join)}\nLeft: {repr(left)}"})

                Connection.execute("DELETE FROM guild_member_cache WHERE guild='Titans Valor'")
                Connection.execute("INSERT INTO guild_member_cache VALUES "+",".join(f"('Titans Valor','{x}')" for x in current_guild_members))
                
                online_all = await Async.get("https://api.wynncraft.com/v3/player")
                online_all = {x for x in online_all["players"]}

                inserts = []

                # get cached members
                cached = {m: g for g, m in Connection.execute("SELECT * FROM guild_member_cache")}
                guilds = {g[0] for g in Connection.execute("SELECT * FROM guild_list")}
                guild_member_cnt = {g: 0 for g in guilds}
                
                for m in cached.keys() & online_all:
                    if not cached[m] in guild_member_cnt: continue
                    guild_member_cnt[cached[m]] += 1

                now = int(time.time())
                Connection.execute("INSERT INTO guild_member_count VALUES" +
                    ','.join(f"(\"{guild}\", {guild_member_cnt[guild]}, {now})" for guild in guild_member_cnt))

                end = time.time()
                logger.info("GUILD ACTIVITY TASK"+f" {end-start}s")
                
                await asyncio.sleep(self.sleep)
        
            logger.info("GuildActivityTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(guild_activity_task))