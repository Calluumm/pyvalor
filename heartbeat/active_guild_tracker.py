import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
import datetime
import time
import sys
from log import logger

class ActiveGuildTrackerTask(Task):
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def active_guild_tracker():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("ACTIVE GUILD TRACKER START")
                start = time.time()

                query = f"""
REPLACE INTO guild_autotrack_active (guild, priority, level)
    (SELECT A.guild, A.priority, IFNULL(B.level, 0) 
FROM
    (SELECT guild, COUNT(*) AS priority
    FROM `player_delta_record` 
    WHERE guild<>"None" AND `time` >= (%s)
    GROUP BY guild
    ORDER BY priority DESC
    LIMIT 50) A
    LEFT JOIN 
    guild_autotrack_active B ON A.guild=B.guild);
"""
                Connection.execute(query, prep_values=[start - 3600*24*7]) 
                
                end = time.time()
                logger.info("ACTIVE GUILD TRACKER"+f" {end-start}s")

                await asyncio.sleep(self.sleep)
        
            logger.info("ActiveGuildTrackerTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(active_guild_tracker))