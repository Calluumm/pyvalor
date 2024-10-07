from .task import Task
from .terr_tracker import TerritoryTrackTask
from .player_activity import PlayerActivityTask
from .gxp_tracker import GXPTrackerTask
from .guild_activity import GuildActivityTask
from .player_stats import PlayerStatsTask
from .guild_tag import GuildTagTask
from .cede_tracker import CedeTrackTask
from .wc_players import WCPlayersTask
from .active_guild_tracker import ActiveGuildTrackerTask
from .season_rating_tracker import SeasonRatingTrackerTask
from dotenv import load_dotenv
import asyncio
import os

load_dotenv()
enabled = os.environ["ENABLED"].lower().split(',')

class Heartbeat:
    wsconns = set()
    cede_tracker = CedeTrackTask(0, 3600*2)

    tasks = [
        TerritoryTrackTask(2, 60, wsconns, cede_tracker),
        PlayerActivityTask(3, 3600),
        GXPTrackerTask(5, 1800),
        GuildActivityTask(61, 300, wsconns),
        PlayerStatsTask(101, 3600),
        GuildTagTask(41, 3600),
        # WCPlayersTask(60),
        # cede_tracker,
        ActiveGuildTrackerTask(29, 3600),
        SeasonRatingTrackerTask(223, 21600)
    ]
    
    @staticmethod
    def run_tasks():
        for t in Heartbeat.tasks:
            if not t.__class__.__name__.lower() in enabled: continue
            t.run()

    @staticmethod
    def stop_tasks():
        for t in Heartbeat.tasks:
            if not t.__class__.__name__.lower() in enabled: continue
            t.stop()
