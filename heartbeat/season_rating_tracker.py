import asyncio
import time
from db import Connection
from network import Async
from log import logger
from .task import Task
import datetime
import re

class SeasonRatingTrackerTask(Task):

    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)

    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def run(self):
        self.finished = False

        async def season_rating_tracker_task():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("SR START")
                start = time.time()

                # Get the top n Guilds
                top_n = 50
                url = f"https://api.wynncraft.com/v3/leaderboards/guildLevel?resultLimit={top_n}"
                guild_data = await Async.get(url)

                top_n_guilds = []
                for guild_info in guild_data.values():
                    top_n_guilds.append(guild_info['name'])

                # Get current season number
                # this relies on callum enterring the season numbers on time
                # res = Connection.execute(
                #     f"SELECT season_name FROM season_list "
                #     f"WHERE start_time <= {current_timestamp} AND end_time >= {current_timestamp}" <-- :thinking:
                #     f"ORDER BY start_time DESC LIMIT 1")    
               
                number_one_guild = guild_data["1"]["name"]
                seasons = (await Async.get(f"https://api.wynncraft.com/v3/guild/{number_one_guild}"))["seasonRanks"]
                current_season_number = max(map(int, seasons.keys()))
                if seasons[str(current_season_number)]["finalTerritories"] > 0:
                    # probably off season unless we genuinely end on zero territories
                    end = time.time()
                    logger.info("SR TRACKER" + f" {end - start}s")
                    await asyncio.sleep(self.sleep)
                    continue
            
                for guild_name in top_n_guilds:
                    guild_url = f"https://api.wynncraft.com/v3/guild/{guild_name}"
                    guild_data = await Async.get(guild_url)
                    current_season_rating = guild_data['seasonRanks'].get(str(current_season_number), {}).get('rating', 0)

                    # Check for the last season rating for the guild
                    last_rating_result = Connection.execute(
                        "SELECT season_rating FROM GuildSeasonRatings "
                        "WHERE guild = %s AND season_id = %s "
                        "ORDER BY timestamp DESC LIMIT 1 ", prep_values=[guild_name, current_season_number])

                    rating_delta = 0
                    if last_rating_result: # pymysql will return () which is falsey
                        last_rating = last_rating_result[0][0]
                        rating_delta = current_season_rating - last_rating

                    # Insert or update the season rating and delta
                    Connection.execute(
                        "INSERT INTO GuildSeasonRatings (guild, season_id, season_rating, rating_delta) "
                        "VALUES(%s, %s, %s, %s)",
                        prep_values=[guild_name, current_season_number, current_season_rating, rating_delta]
                    )

                    await asyncio.sleep(0.3)

                end = time.time()
                logger.info("SR TRACKER" + f" {end - start}s")
                await asyncio.sleep(self.sleep)

            logger.info("SRTrackerTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(season_rating_tracker_task))
