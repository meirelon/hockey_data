from extract_utilities import nhl_data_requests as ndr
import pandas as pd

import settings
import progress_bar as pb

import uuid


class Stats:
    def __init__(self, data_since, situational=False, collect=False, expand=None) -> None:
        self.data_since = data_since
        self.situational = situational
        self.collect = collect
        self.expand = expand
    files = settings.get_data_file_paths()
    if "metadata.csv" not in files:
        raise Exception("Please run roster metadata")
    player_metadata = pd.read_csv(files["metadata.csv"], usecols=["id", "primaryPosition_code"])
 
    def all_player_stats(self, player_type):
        if player_type not in ["skater", "goalie"]:
            raise Exception("Please specifiy skater or goalie stat type")
        if player_type == "skater":
            mask = ['C', 'L', 'D', 'R']
        if player_type == "goalie":
            mask = ['G']
        
        directory = "data/stats"
        temp_directory = f"data/stats/{player_type}"

        players = list(self.player_metadata.loc[self.player_metadata["primaryPosition_code"].isin(mask)]["id"].unique())
        n = len(players)

        for i, player in enumerate(players):
            player_stats = (pb.report(i, n), self.player_stats(player))[1]
            settings.write_df_to_csv(player_stats, temp_directory, f"{player}.csv")
        player_df = pd.concat([pd.read_csv(file)
                               for file in settings.wildcard_files(f"{temp_directory}/*.csv")])
        settings.write_df_to_csv(player_df, directory, f"{player_type}s.csv")
        settings.remove_folder(temp_directory)

        return True
        
    def player_stats(self, player_id, stats_type="yearByYear"):
        url = f"https://statsapi.web.nhl.com/api/v1/people/{player_id}/stats?stats={stats_type}"
        body = ndr.get_request(url)

        stats = pd.concat([pd.json_normalize(b, sep="_") for b in body["stats"][0]["splits"]])
        stats.columns = stats.columns.str.replace("stat_", "")
        stats["player_id"] = player_id

        if self.situational:
            seasons = stats["season"].unique()
            situational_stats = self.player_situational_goals(player_id, seasons)
            if isinstance(situational_stats, pd.DataFrame):
                return stats.join(situational_stats, on="season", how="left", rsuffix="situational")
        return stats

    def player_situational_goals(self, player_id, seasons):
        situational_list = []
        for season in seasons:
            url = f"https://statsapi.web.nhl.com/api/v1/people/{player_id}/stats?stats=goalsByGameSituation&season={season}"
            body = ndr.get_request(url)
            if body["stats"][0]["splits"]:
                situational_df = pd.DataFrame(body["stats"][0]["splits"][0]["stat"], index=["season"])
                situational_df["season"] = season
                situational_list.append(situational_df)
        if situational_list:
            return pd.concat(situational_list).set_index("season")

    def _gamepks(self):
        files = settings.get_data_file_paths()
        if "schedules.csv" not in files:
            raise Exception("Please run schedules")
            
        schedules = pd.read_csv("data/schedules/schedules.csv")
        GAMEPKS = schedules[schedules["gameDate"] >= self.data_since].gamePk.unique()
        return GAMEPKS

    def _sub_df_transform(player_id, splits):
        df = pd.json_normalize(splits, sep="_")
        df["player_id"] = player_id
        return df

    def nhl_live(self):
        GAMEPKS = self._gamepks()
        n = len(GAMEPKS)
        for i, GAMEPK in enumerate(GAMEPKS):
            pb.report(i, n)
            base_url = 'https://statsapi.web.nhl.com'
            game_id = GAMEPK

            url = f'{base_url}/api/v1/game/{game_id}/feed/live'
            body = ndr.get_request(url)
            events = body['liveData']['plays']['allPlays']
            df = pd.json_normalize(events, sep="_")
            df["event_uuid"] = [uuid.uuid4().hex for _ in range(len(df.index))]
            df["gamepk"] = GAMEPK
            settings.write_df_to_storage(df, fp="data/live_temp", fn=f"{GAMEPK}", file_type="csv")
            if self.collect:
                settings.temp_files_to_df(wildcard="*.csv", fp_temp="data/live_temp", fp_final="data/stats", fn="live_games")
        return "Data stored in data/live/live_game_stats.csv"

