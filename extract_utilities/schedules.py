import extract_utilities.nhl_data_requests as ndr
import pandas as pd
import settings


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
        for t in teams_df[["mostRecentTeamId", "firstSeasonId", "lastSeasonId"]].apply(lambda x: x.astype(str)).itertuples(index=False):
            self.schedules_single_team_all_seasons(t[0], t[1], t[2].replace(".0", ""))
        if self.collect:
            settings.temp_files_to_df(wildcard="*.csv", fp_temp=f"{directory}_temp", fp_final=directory, fn=file_name)

    def schedules_single_team_all_seasons(self, team, first_season, last_season):
        seasons = ndr.generate_season_array(first_season, last_season)
        for season in seasons:
            schedule = self.get_team_schedule(team, season)
            if schedule is not None:
                settings.write_df_to_storage(schedule, fp=f"data/{team}_temp", fn=season, file_type="csv")
        settings.temp_files_to_df(wildcard="*.csv", fp_temp=f"data/{team}_temp", fp_final="data/schedules_temp", fn=team)

    def get_team_schedule(self, team_id, season):
        url = f"https://statsapi.web.nhl.com/api/v1/schedule?teamId={team_id}&season={season}"
        if self.expand and isinstance(self.expand, (str, list)):
            expand = ",".join(self.expand) if isinstance(self.expand, list) else self.expand
            url = url + f"&expand={expand}"
        body = ndr.get_request(url)
        games = [pd.json_normalize(g["games"], sep="_") for g in body["dates"]]
        if games:
            return pd.concat(games)
        else:
            raise Exception("expand must be either str or list")
