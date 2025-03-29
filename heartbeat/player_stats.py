import asyncio
import aiohttp
from db import Connection
from network import Async
from .task import Task
import time
import datetime
import sys
from dotenv import load_dotenv
import json
import math
import os
from log import logger

load_dotenv()
api_key = os.environ["API_KEY"]

class PlayerStatsTask(Task):
    idx = {'uuid': 0, 'firstjoin': 1, 'Decrepit Sewers': 2, 'Infested Pit': 3, 'Lost Sanctuary': 4, 'Underworld Crypt': 5, 
               'Sand-Swept Tomb': 6, 'Ice Barrows': 7, 'Undergrowth Ruins': 8, "Galleon's Graveyard": 9, 'Fallen Factory': 10, 
               'Eldritch Outlook': 11,'Corrupted Decrepit Sewers': 12, 'Corrupted Infested Pit': 13, 'Corrupted Lost Sanctuary': 14, 
               'Corrupted Underworld Crypt': 15, 'Corrupted Sand-Swept Tomb': 16, 'Corrupted Ice Barrows': 17, 'Corrupted Undergrowth Ruins': 18, 
               'itemsIdentified': 19, 'chestsFound': 20, 'blocksWalked': 21, 'logins': 22, 'playtime': 23, 'alchemism': 24, 'armouring': 25, 
               'combat': 26, 'cooking': 27, 'farming': 28, 'fishing': 29, 'jeweling': 30, 'mining': 31, 'scribing': 32, 'tailoring': 33, 
               'weaponsmithing': 34, 'woodcutting': 35, 'woodworking': 36, 'Nest of the Grootslangs': 37, 'The Canyon Colossus': 38, 
               "mobsKilled": 39, "deaths": 40, "guild": 41, "Orphion's Nexus of Light": 42, "guild_rank": 43, "The Nameless Anomaly": 44, 
               "Corrupted Galleon's Graveyard": 45, "Timelost Sanctum": 46, "lastjoin": 47}
    
    global_stats_threshold = {"g_killedMobs": 2500, "g_chestsFound": 20, "g_totalLevel": 3}
    
    def __init__(self, start_after, sleep):
        super().__init__(start_after, sleep)
        
    def stop(self):
        self.finished = True
        self.continuous_task.cancel()

    def null_or_value(x): 
        if type(x) == type(None): return 0
        return x

    async def get_uuid(player: str):
        if "-" in player: return False
        exist = Connection.execute(f"SELECT * FROM uuid_name WHERE name='{player}' LIMIT 1")
        if not exist:
            mojang_data = await Async.get(f"https://api.mojang.com/users/profiles/minecraft/{player}")
            if not "id" in mojang_data:
                return False
            
            uuid = mojang_data["id"]
            uuid36 = uuid[:8]+'-'+uuid[8:12]+'-'+uuid[12:16]+'-'+uuid[16:20]+'-'+uuid[20:]
            Connection.execute(f"INSERT INTO uuid_name VALUES ('{uuid36}', '{player}')")
        else:
            return exist[0][0]
        return uuid36

    @staticmethod
    def profession_level_to_xp(level):
        # "profesisons_level_to_xp(5.5) is not the same at level 5 and 50% of the way to level 6" type of situation
        assert isinstance(level, int), "Level needs to be an int (msg Andrew for xp percent calculations)"
        return math.floor(369.525*math.exp(0.108856*level))
    
    @staticmethod
    def lvl_pct_to_xp(level, xpPercent):
        xp_to_curr = PlayerStatsTask.profession_level_to_xp(level)
        xp_to_next = PlayerStatsTask.profession_level_to_xp(level + 1)
        curr_xp = xpPercent * (xp_to_next - xp_to_curr) + xp_to_curr

        return curr_xp

    @staticmethod
    def append_player_global_stats_feature(feature_list, now, uuid, guild, kv_dict, old_global_stats, update_player_global_stats, deltas_player_global_stats, prefix="g"):
        old_player_global_stats = old_global_stats.get(uuid)
        for feat in feature_list:
            feat_name = f"{prefix}_{feat}"
            new_val = kv_dict[feat]
            delta_val = (new_val - old_player_global_stats[feat_name]) if old_player_global_stats and feat_name in old_player_global_stats else 0
            if delta_val > 0:
                if not feat_name in PlayerStatsTask.global_stats_threshold or delta_val >= PlayerStatsTask.global_stats_threshold[feat_name]:
                    deltas_player_global_stats.append((uuid, guild, now, feat_name, delta_val))
            update_player_global_stats.append((uuid, feat_name, new_val))
        
    @staticmethod 
    def append_player_global_stats(stats, old_global_data, update_player_global_stats, deltas_player_global_stats):
        try:
            global_data_features = ["wars", "totalLevel", "killedMobs", "chestsFound", "completedQuests"]
            global_data_dungeons_features = [*stats["globalData"]["dungeons"]["list"].keys()]
            global_data_raids_features = [*stats["globalData"]["raids"]["list"].keys()]
            global_data_pvp_features = ["kills", "deaths"]
            now = time.time()

            uuid = stats["uuid"]
            guild = stats["guild"]["name"] if stats["guild"] else "None"
            PlayerStatsTask.append_player_global_stats_feature(global_data_features, now, uuid, guild, stats["globalData"], old_global_data, update_player_global_stats, deltas_player_global_stats)
            PlayerStatsTask.append_player_global_stats_feature(global_data_dungeons_features, now, uuid, guild, stats["globalData"]["dungeons"]["list"], old_global_data, update_player_global_stats, deltas_player_global_stats)
            PlayerStatsTask.append_player_global_stats_feature(global_data_raids_features, now, uuid, guild, stats["globalData"]["raids"]["list"], old_global_data, update_player_global_stats, deltas_player_global_stats)
            PlayerStatsTask.append_player_global_stats_feature(global_data_pvp_features, now, uuid, guild, stats["globalData"]["pvp"], old_global_data, update_player_global_stats, deltas_player_global_stats)

            # Sum character-exclusive stats to get new global stats
            character_uuids = [*stats["characters"].keys()]
            character_features = ["playtime", "logins", "deaths", "discoveries"]

            character_stats = {}
            character_stats["professions"] = {}

            for character_uuid in character_uuids:
                character_data = stats["characters"][character_uuid]

                for character_feature in character_features:
                    character_stats[character_feature] = character_stats.get(character_feature, 0) + PlayerStatsTask.null_or_value(character_data.get(character_feature))
                
                for profession in [*stats["characters"][character_uuid]["professions"].keys()]:
                    character_prof_data = character_data.get("professions", {}).get(profession)
                    if character_prof_data is None: continue
                    character_prof_xp = PlayerStatsTask.lvl_pct_to_xp(character_prof_data.get("level", 1), PlayerStatsTask.null_or_value(character_prof_data.get("xpPercent")) / 100)
                    character_stats["professions"][profession] = character_stats["professions"].get(profession, 0) + character_prof_xp

            PlayerStatsTask.append_player_global_stats_feature(character_features, now, uuid, guild, character_stats, old_global_data, update_player_global_stats, deltas_player_global_stats, "c")
            PlayerStatsTask.append_player_global_stats_feature([*character_stats["professions"].keys()], now, uuid, guild, character_stats["professions"], old_global_data, update_player_global_stats, deltas_player_global_stats, "c")

        except Exception as e:
            logger.exception(e)
            logger.warn(f"PLAYER STATS could not append global data for {stats['uuid']}")
        
    @staticmethod
    async def track_player(player, old_membership, prev_warcounts, old_global_data, inserts_war_update, inserts_war_deltas, inserts_guild_log, inserts, uuid_name, update_player_global_stats, deltas_player_global_stats) -> bool:
        uri = f"https://api.wynncraft.com/v3/player/{player}?fullResult=True"
        try:
            stats = await Async.get(uri)
            first_key = [*stats][0]
            if "storedName" in stats[first_key]: # there are multiple players so select the first any with a rank
                rank_order = dict(enumerate([None, "vip", "vipplus", "hero", "champion"]))
                players_sorted_by_rank = sorted([*stats], key=lambda x: rank_order.get(x, -1), reverse=True) 
                player = players_sorted_by_rank[0]
                uri = f"https://api.wynncraft.com/v3/player/{player}?fullResult=True"
                stats = await Async.get(uri)
        except:
            uuid = await PlayerStatsTask.get_uuid(player)
            
            try:
                uri = f"https://api.wynncraft.com/v3/player/{uuid}?fullResult=True"
                stats = await Async.get(uri)
            except:
                logger.warn(f"PLAYER STATS uuid and name don't work: {player}")
                return False

        row = [0]*len(PlayerStatsTask.idx)
        if not stats or not "uuid" in stats:
            return False

        uuid = stats["uuid"]
        row[PlayerStatsTask.idx["uuid"]] = uuid
        player = stats["username"] # make sure player becomes username

        if not "lastJoin" in stats: 
            return False

        row[PlayerStatsTask.idx["lastjoin"]] = datetime.datetime.fromisoformat(stats["lastJoin"][:-1]).timestamp()

        if not "firstJoin" in stats: 
            return False

        row[PlayerStatsTask.idx["firstjoin"]] = datetime.datetime.fromisoformat(stats["firstJoin"][:-1]).timestamp()

        if not "characters" in stats:
            return False
        
        PlayerStatsTask.append_player_global_stats(stats, old_global_data, update_player_global_stats, deltas_player_global_stats)

        guild = None
        guild_rank = None
        if stats["guild"]:
            guild = stats["guild"]["name"]
            guild_rank = stats["guild"]["rank"]
        
        old_guild, old_rank = old_membership.get(uuid, [None, None])
        if guild != old_guild:
            inserts_guild_log.append(f"('{uuid}', '{old_guild}', '{old_rank}', '{guild}', {time.time()})")

        row[PlayerStatsTask.idx["guild"]] = f'"{guild}"'
        row[PlayerStatsTask.idx["guild_rank"]] = f'"{guild_rank}"'

        character_data = stats["characters"]
        for cl_name in character_data:
            cl = character_data[cl_name]
            cl_type = cl["type"]

            warcount = PlayerStatsTask.null_or_value(cl.get("wars", 0))
            if uuid in prev_warcounts and cl_name in prev_warcounts[uuid]:
                old_warcount = prev_warcounts[uuid][cl_name]
                # if war count hasn't changed don't update a thing
                if warcount != old_warcount:
                    inserts_war_deltas.append((uuid, cl_name, warcount-old_warcount, cl_type))
                    inserts_war_update.append((uuid, cl_name, warcount, cl_type))
            else:
                inserts_war_update.append((uuid, cl_name, warcount, cl_type))

            if cl["dungeons"]:
                for dung, dung_count in cl["dungeons"]["list"].items():
                    if dung in PlayerStatsTask.idx:
                        row[PlayerStatsTask.idx[dung]] += dung_count

            if cl["raids"]:
                for raid, raid_count in cl["raids"]["list"].items():
                    if raid in PlayerStatsTask.idx:
                        row[PlayerStatsTask.idx[raid]] += raid_count

            row[PlayerStatsTask.idx["itemsIdentified"]] += PlayerStatsTask.null_or_value(cl.get("itemsIdentified", 0))
            row[PlayerStatsTask.idx["mobsKilled"]] += PlayerStatsTask.null_or_value(cl.get("mobsKilled", 0))
            row[PlayerStatsTask.idx["chestsFound"]] += PlayerStatsTask.null_or_value(cl.get("chestsFound", 0))
            row[PlayerStatsTask.idx["blocksWalked"]] += PlayerStatsTask.null_or_value(cl.get("blocksWalked", 0))
            row[PlayerStatsTask.idx["logins"]] += PlayerStatsTask.null_or_value(cl.get("logins", 0))
            row[PlayerStatsTask.idx["deaths"]] += PlayerStatsTask.null_or_value(cl.get("deaths", 0))
            row[PlayerStatsTask.idx["playtime"]] += PlayerStatsTask.null_or_value(cl.get("playtime", 0))
            # row[idx["combat"]] += cl["level"] todo combat lvl is gone
            
            if not cl.get("professions"): 
                continue

            for prof in cl.get("professions"):
                if not "xpPercent" in cl["professions"][prof]: continue
                if not prof in PlayerStatsTask.idx: 
                    logger.warn(f"PLAYER STATS cannot find prof {prof} player {player}")
                    continue

                xp = cl["professions"][prof]["xpPercent"]
                row[PlayerStatsTask.idx[prof]] += cl["professions"][prof]["level"] + (xp if xp else 0)/100
        
        inserts.append(row)
        uuid_name.append((uuid, player))
        return True

    @staticmethod
    async def get_stats_track_references(needs_player_list=True, force_player_list=[]):
        if needs_player_list:
            online_all = await Async.get("https://api.wynncraft.com/v3/player")
        else: 
            online_all = {}
        online_all = {name for name in online_all.get("players", [])}
        online_all = online_all | set(force_player_list)

        already_uuid = [x for x in online_all if '-' in x]
        online_all = online_all - set(already_uuid)

        queued_players = [] # is this used? [x[0] for x in Connection.execute("SELECT uuid FROM player_stats_queue")]
        search_players = list(online_all | set(queued_players))[::-1]

        # search_players_clause = '(' + ','.join(f'"{name}"' for name in online_all) + ')'
        search_players_clause = '(' + ('%s,'*len(online_all))[:-1] + ')'
        # search_uuids_clause = '(' + ','.join(f'"{uuid}"' for uuid in queued_players) + ')'
        search_uuids_clause = '(' + ('%s,'*len(queued_players))[:-1] + ')'

        existing_player_uuids = []
        if online_all:
            existing_player_uuids = [x[0] for x in 
                Connection.execute(f"SELECT uuid FROM uuid_name WHERE name IN {search_players_clause}" + \
                                (f" OR uuid IN {search_uuids_clause}" if queued_players else ""), prep_values=list(online_all) + queued_players)]
        
        existing_player_uuids.extend(already_uuid)
        # existing_uuids_clause = '(' + ','.join(f'"{uuid}"' for uuid in existing_player_uuids) + ')'
        existing_uuids_clause = '(' + ("%s,"*len(existing_player_uuids))[:-1] + ')'
        # search_players = [x[0] for x in Connection.execute("SELECT * FROM `player_stats` ORDER BY playtime DESC LIMIT 10000;")][5000:]

        old_membership = {}
        res = Connection.execute(f"SELECT uuid, guild, guild_rank FROM `player_stats` WHERE guild IS NOT NULL and guild != 'None' and guild != '' AND uuid IN {existing_uuids_clause}",
                                prep_values=existing_player_uuids)
        for uuid, guild, guild_rank in res:
            old_membership[uuid] = [guild, guild_rank]

        res = Connection.execute(f"SELECT uuid, character_id, time, warcount FROM cumu_warcounts WHERE uuid IN {existing_uuids_clause}",
                                prep_values=existing_player_uuids)
        prev_warcounts = {}
        for uuid, character_id, _, warcount in res:
            if not uuid in prev_warcounts:
                prev_warcounts[uuid] = {}
            prev_warcounts[uuid][character_id] = warcount
        
        res = Connection.execute(f"SELECT uuid, label, value FROM player_global_stats WHERE uuid IN {existing_uuids_clause}",
                                prep_values=existing_player_uuids)
        old_global_data = {}
        for uuid, label, value in res:
            if not uuid in old_global_data:
                old_global_data[uuid] = {}
            
            old_global_data[uuid][label] = value

        return search_players, old_membership, prev_warcounts, old_global_data

    @staticmethod
    def get_empty_stats_track_buffers():
        inserts_war_update = []
        inserts_war_deltas = []
        inserts_guild_log = []
        inserts = []
        uuid_name = []
        update_player_global_stats = []
        deltas_player_global_stats = []

        return inserts_war_update, inserts_war_deltas, inserts_guild_log, inserts, uuid_name, update_player_global_stats, deltas_player_global_stats

    @staticmethod
    def write_results_to_db(inserts_war_update, inserts_war_deltas, inserts_guild_log, inserts, uuid_name, update_player_global_stats, deltas_player_global_stats):
        if inserts:
            curr_time = time.time()
            query_stats = "REPLACE INTO player_stats VALUES " + ','.join(f"('{x[0]}', {str(x[1])}, {','.join(map(str, x[2:]))})" for x in inserts)
            query_uuid = "REPLACE INTO uuid_name VALUES " + ','.join(f"(\'{uuid}\',\'{name}\')" for uuid, name in uuid_name)
            query_wars_update  = "REPLACE INTO cumu_warcounts VALUES " + ','.join(f"(\'{uuid}\',\'{character_id}\', {curr_time}, {warcount}, \'{cl_type}\')" 
                                                                                    for uuid, character_id, warcount, cl_type in inserts_war_update)
            query_wars_delta  = "INSERT INTO delta_warcounts VALUES " + ','.join(f"(\'{uuid}\',\'{character_id}\', {curr_time}, {wardiff}, \'{cl_type}\')" 
                                                        for uuid, character_id, wardiff, cl_type in inserts_war_deltas)
            query_global_delta  = "INSERT INTO player_delta_record VALUES " + ','.join(f"(\'{uuid}\',\'{guild}\', {now}, " + '"'+feat_name+'"' + f", {delta_val})" 
                                                        for uuid, guild, now, feat_name, delta_val in deltas_player_global_stats)
            query_global_update  = "REPLACE INTO player_global_stats VALUES " + ',\n'.join(f"(\'{uuid}\'," + '"'+feat_name+'"'+f", {value})" 
                                                        for uuid, feat_name, value in update_player_global_stats)

            if inserts_war_update:
                Connection.execute(query_wars_update)
            if inserts_war_deltas:
                Connection.execute(query_wars_delta)
            if update_player_global_stats:
                Connection.execute(query_global_update)
            if deltas_player_global_stats:
                Connection.execute(query_global_delta)

            name_paren = ['\''+uuid+'\'' for uuid, _ in uuid_name]
            old_names = Connection.execute(
                f"SELECT uuid, name FROM uuid_name WHERE uuid IN ({','.join(name_paren)})")
            old_names_dict = {uuid: old for uuid, old in old_names} # believe me, this way is still faster than tmp table join
            uuid_name_history_update = []
            for uuid, name in uuid_name:
                if uuid in old_names_dict and old_names_dict[uuid] != name:
                    uuid_name_history_update.append((uuid, old_names_dict[uuid], name, curr_time))
            if uuid_name_history_update:
                query_uuid_name_history = "INSERT INTO uuid_name_history VALUES " + \
                    ','.join(f"('{uuid}','{old}','{new}',{curr_time})" for uuid, old, new, curr_time in uuid_name_history_update)
                Connection.execute(query_uuid_name_history)

            Connection.execute(query_stats)
            Connection.execute(query_uuid)
            
        if inserts_guild_log:
            query_guild_log = "INSERT INTO guild_join_log VALUES " + ','.join(inserts_guild_log)
            Connection.execute(query_guild_log)

    def run(self):
        self.finished = False
        
        async def player_stats_task():
            await asyncio.sleep(self.start_after)

            while not self.finished:
                logger.info("PLAYER STATS TRACK START")
                start = time.time()

                # generic "inserts" are actually REPLACE INTOs into player_stats table
                inserts_war_update, inserts_war_deltas, inserts_guild_log, inserts, uuid_name, update_player_global_stats, deltas_player_global_stats = PlayerStatsTask.get_empty_stats_track_buffers()
                search_players, old_membership, prev_warcounts, old_global_data = await PlayerStatsTask.get_stats_track_references()

                cnt = 0
                player_idx = 0

                while player_idx < len(search_players):
                    try:
                        player = search_players[player_idx] # it could be uuid or name
                        if not await PlayerStatsTask.track_player(player, old_membership, prev_warcounts, old_global_data, inserts_war_update, inserts_war_deltas, inserts_guild_log, inserts, uuid_name, update_player_global_stats, deltas_player_global_stats):
                            player_idx += 1
                        cnt += 1

                        if (cnt % 10 == 0 or player_idx == len(search_players)-1):
                            PlayerStatsTask.write_results_to_db(inserts_war_update, inserts_war_deltas, inserts_guild_log, inserts, uuid_name, update_player_global_stats, deltas_player_global_stats)
                            inserts_war_update, inserts_war_deltas, inserts_guild_log, inserts, uuid_name, update_player_global_stats, deltas_player_global_stats = PlayerStatsTask.get_empty_stats_track_buffers()

                        await asyncio.sleep(0.6)

                    except Exception as e:
                        logger.info(f"PLAYER STATS TASK ERROR")
                        logger.exception(e)
                        print(f"PLAYER IS {search_players[player_idx]}")
                    
                    player_idx += 1

                end = time.time()
                logger.info("PLAYER STATS TASK"+f" {end-start}s")
                Connection.execute("DELETE FROM player_stats_queue") 
                
                await asyncio.sleep(self.sleep)
        
            logger.info("PlayerStatsTask finished")

        self.continuous_task = asyncio.get_event_loop().create_task(self.continuously(player_stats_task))
