import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
from collections import defaultdict
import time
import datetime
from log import logger

class PlayerLastJoinTask(Task):
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False
        async def player_activity_task():
            await asyncio.sleep(self.start_after)

            logger.info("PLAYER LAST JOIN TASK TRACK START")
            start = time.time()
            online_all = await Async.get("https://api.wynncraft.com/v3/player")
            online_all = [x for x in online_all["players"]]

            for i in range(0, len(online_all), 512):
                player_subset = online_all[i:i+512]

                # res = Connection.execute(f"SELECT name, uuid FROM uuid_name WHERE name IN ({('%s,'*len(player_subset))[:-1]})",
                #                     prep_values=player_subset)
                
                uuid_last_join_pairs_flat = []
                # name_to_uuid = dict(res)
                for player_name in player_subset:
                    # if player_name not in name_to_uuid:
                    #     continue
                    # uuid = name_to_uuid[player_name]
                    uuid_last_join_pairs_flat.append(player_name)
                    uuid_last_join_pairs_flat.append(int(start))
                
#                 update_query = f"""
# UPDATE player_stats A
#     JOIN uuid_name B ON A.uuid=B.uuid
#     JOIN (
#     	VALUES
#         {("ROW(%s,%s),"*(len(uuid_last_join_pairs_flat)//2))[:-1]}
#     ) C(name, lastjoin) ON C.name=B.name
# SET A.lastjoin = C.lastjoin;
# """

                update_query = f"""
REPLACE INTO player_last_join
VALUES
   {("(%s, %s)," * (len(uuid_last_join_pairs_flat)//2))[:-1]};
"""
                if uuid_last_join_pairs_flat:
                    Connection.execute(update_query, prep_values=uuid_last_join_pairs_flat)

            end = time.time()
            logger.info("PLAYER LAST JOIN TASK"+f" {end-start}s")
            
            await asyncio.sleep(self.sleep)

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(player_activity_task))
        
