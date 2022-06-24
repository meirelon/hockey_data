import ast
from extract_utilities import nhl_data_requests as ndr
import pandas as pd
import numpy as np

import settings
import progress_bar as pb

import uuid


def event_to_df(players: str, gamePk: int, event_id: str) -> pd.DataFrame:
    players = ast.literal_eval(players) if isinstance(players, str) else None
    if not players:
        return None
    df = pd.concat([pd.DataFrame(p["player"], index=[0]) for p in players])
    event_types = [p["playerType"] for p in players]
    df["event_uuid"] = event_id
    df["gamepk"] = gamePk
    df["event_type"] = event_types
    return df.reset_index(drop=True)


class Stats:
    def __init__(
        self,
        stats_types=None,
        data_since=None,
        collect=None,
        expand=None,
        season_level_request=None,
    ) -> None:
        self.data_since = data_since
        self.stats_types = stats_types
        self.collect = collect
        self.expand = expand
        self.season_level_request = season_level_request

    files = settings.get_data_file_paths()
    if "metadata.csv" not in files:
        raise Exception("Please run roster metadata")
    if "rosters.csv" not in files:
        raise Exception("Please run rosters")
    player_metadata = pd.read_csv(
        files["metadata.csv"], usecols=["id", "primaryPosition_code"]
    )
    player_seasons = (
        pd.read_csv("data/rosters/rosters.csv", usecols=["person_id", "season"])
        .groupby("person_id")
        .agg({"season": [np.min, np.max]})
        .reset_index()
    )

    def all_player_stats(self, player_type, level="season_by_season"):
        if player_type not in ["skater", "goalie"]:
            raise Exception("Please specifiy skater or goalie stat type")
        if player_type == "skater":
            mask = ["C", "L", "D", "R"]
        if player_type == "goalie":
            mask = ["G"]

        players = list(
            self.player_metadata.loc[
                self.player_metadata["primaryPosition_code"].isin(mask)
            ]["id"].unique()
        )
        n = len(players)

        for i, player in enumerate(players):
            pb.report(i, n)
            if self.season_level_request:
                self.player_stats_all_seasons(player)
            else:
                self.player_stats(player)

        if self.collect:
            for stat_type in self.stats_types:
                directory = f"data/stats/{stat_type}"
                temp_directory = f"data/stats/{stat_type}_temp"
                settings.temp_files_to_df(
                    wildcard="*.csv",
                    fp_temp=temp_directory,
                    fp_final=directory,
                    fn=player_type + "s",
                )
        return True

    def player_stats_all_seasons(self, player_id):
        player = self.player_seasons[self.player_seasons["person_id"].isin([player_id])]
        seasons = ndr.generate_season_array(
            str(player.season.amin.values[0]), str(player.season.amax.values[0])
        )
        for season in seasons:
            self._player_stats_by_season(player_id=player_id, season=season)

    def _player_stats_by_season(self, player_id, season):
        stats_types = (
            ",".join(self.stats_types)
            if isinstance(self.stats_types, list)
            else self.stats_types
        )
        url = f"https://statsapi.web.nhl.com/api/v1/people/{player_id}/stats/?stats={stats_types}&season={season}"
        body = ndr.get_request(url)
        if not body:
            return None
        stats = {
            b["type"]["displayName"]: self._sub_df_transform(
                player_id=player_id, splits=b["splits"]
            )
            for b in body["stats"]
            if b and b.get("type", False)
        }

        for k, v in stats.items():
            settings.write_df_to_storage(
                df=v,
                fp=f"data/stats/{k}_temp",
                fn=f"{player_id}_{season}",
                file_type="csv",
            )
        return True

    def player_stats(self, player_id):
        stats_types = (
            ",".join(self.stats_types)
            if isinstance(self.stats_types, list)
            else self.stats_types
        )
        url = f"https://statsapi.web.nhl.com/api/v1/people/{player_id}/stats?stats={stats_types}"
        body = ndr.get_request(url)
        stats = {
            b["type"]["displayName"]: self._sub_df_transform(
                player_id=player_id, splits=b["splits"]
            )
            for b in body["stats"]
            if b and b.get("type", False)
        }

        for k, v in stats.items():
            settings.write_df_to_storage(
                df=v, fp=f"data/stats/{k}_temp", fn=f"{player_id}", file_type="csv"
            )
        return True

    def _gamepks(self):
        files = settings.get_data_file_paths()
        if "schedules.csv" not in files:
            raise Exception("Please run schedules")

        schedules = pd.read_csv("data/schedules/schedules.csv")
        GAMEPKS = schedules[schedules["gameDate"] >= self.data_since].gamePk.unique()
        return GAMEPKS

    def _sub_df_transform(self, player_id, splits):
        df = pd.json_normalize(splits, sep="_")
        df["player_id"] = player_id
        return df

    def nhl_live(self):
        GAMEPKS = self._gamepks()
        n = len(GAMEPKS)
        for i, GAMEPK in enumerate(GAMEPKS):
            pb.report(i, n)
            base_url = "https://statsapi.web.nhl.com"
            game_id = GAMEPK

            url = f"{base_url}/api/v1/game/{game_id}/feed/live"
            body = ndr.get_request(url)
            events = body["liveData"]["plays"]["allPlays"]
            df = pd.json_normalize(events, sep="_")
            df["event_uuid"] = [uuid.uuid4().hex for _ in range(len(df.index))]
            df["gamepk"] = GAMEPK
            settings.write_df_to_storage(
                df, fp="data/live_temp", fn=f"{GAMEPK}", file_type="csv"
            )
            if self.collect:
                settings.temp_files_to_df(
                    wildcard="*.csv",
                    fp_temp="data/live_temp",
                    fp_final="data/stats",
                    fn="live_games",
                )
        return "Data stored in data/live/live_games.csv"

    def unnest_live_events(self):
        iter_csv = pd.read_csv(
            "data/stats/liveGames/live_games.csv",
            iterator=True,
            chunksize=10000,
            usecols=["players", "gamepk", "event_uuid", "coordinates_x"],
        )
        for chunk in iter_csv:
            for _, row in chunk[~chunk["coordinates_x"].isna()].iterrows():
                df = event_to_df(row["players"], row["gamepk"], row["event_uuid"])
                if df is not None:
                    settings.write_df_to_storage(
                        df,
                        fp="data/stats/liveGames_temp/",
                        fn=row["event_uuid"],
                        file_type="csv",
                    )
        if self.collect:
            settings.temp_files_to_df(
                wildcard="*.csv",
                fp_temp="data/stats/liveGames_temp",
                fp_final="data/stats/liveGames",
                fn="live_games_unnested",
            )
