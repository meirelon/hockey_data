import ast
import uuid
import extract_utilities.nhl_data_requests as ndr
import pandas as pd
import settings
import progress_bar as pb


def scoringplay_to_df(scoring_play: str, gamePk: int) -> pd.DataFrame:
    scoring_play_list = ast.literal_eval(scoring_play)
    if not scoring_play_list:
        return None
    df = pd.concat([pd.json_normalize(s, sep="_") for s in scoring_play_list])
    df["event_id"] = [uuid.uuid4().hex for _ in range(len(df))]
    df["gamePk"] = gamePk
    df = df.explode("players").reset_index(drop=True)
    players = pd.concat(
        [pd.DataFrame(p["player"], index=[0]) for p in df["players"]]
    ).reset_index(drop=True)
    scoringplay_df = pd.concat([players, df.drop("players", axis=1)], axis=1)
    settings.append_df(scoringplay_df, "data/stats/scoringplays/scoringplays.csv")
    # scoringplay_df.to_csv(
    #     "data/stats/scoringplays/scoringplays.csv", mode="a", index=False
    # )


class Schedules:
    def __init__(self, expand=None, collect=False):
        self.expand = expand
        self.collect = collect

    files = settings.get_data_file_paths()

    def schedules_all_teams_all_seasons(self, file_name="schedules"):
        if self.expand and isinstance(self.expand, list):
            file_name = self.expand[0].replace(".", "_")
        teams_df = ndr.get_teams(current_only=False)
        directory = "data/schedules"
        for t in (
            teams_df[["mostRecentTeamId", "firstSeasonId", "lastSeasonId"]]
            .apply(lambda x: x.astype(str))
            .itertuples(index=False)
        ):
            self.schedules_single_team_all_seasons(t[0], t[1], t[2].replace(".0", ""))
        if self.collect:
            settings.temp_files_to_df(
                wildcard="*.csv",
                fp_temp=f"{directory}_temp",
                fp_final=directory,
                fn=file_name,
            )

    def schedules_single_team_all_seasons(self, team, first_season, last_season):
        seasons = ndr.generate_season_array(first_season, last_season)
        n = len(seasons)
        for i, season in enumerate(seasons):
            pb.report(i, n)
            schedule = self.get_team_schedule(team, season)
            if schedule is not None:
                settings.write_df_to_storage(
                    schedule, fp=f"data/{team}_temp", fn=season, file_type="csv"
                )
        settings.temp_files_to_df(
            wildcard="*.csv",
            fp_temp=f"data/{team}_temp",
            fp_final="data/schedules_temp",
            fn=team,
        )

    def get_team_schedule(self, team_id, season):
        url = f"https://statsapi.web.nhl.com/api/v1/schedule?teamId={team_id}&season={season}"
        if self.expand and isinstance(self.expand, (str, list)):
            expand = (
                ",".join(self.expand) if isinstance(self.expand, list) else self.expand
            )
            url = url + f"&expand={expand}"
        body = ndr.get_request(url)
        games = [pd.json_normalize(g["games"], sep="_") for g in body["dates"]]
        if games:
            return pd.concat(games)

    def unnest_scoringplays(self, directory="data/stats/scoringplays"):
        settings.remove_folder(directory)
        settings.create_folder(directory)
        iter_csv = pd.read_csv(
            "data/schedules/schedule_scoringplays.csv",
            iterator=True,
            chunksize=1000,
            usecols=["scoringPlays", "gamePk"],
        )
        for chunk in iter_csv:
            for _, row in chunk.iterrows():
                scoringplay_to_df(row["scoringPlays"], row["gamePk"])
