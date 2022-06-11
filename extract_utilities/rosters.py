import extract_utilities.nhl_data_requests as ndr
import pandas as pd
import settings
import progress_bar as pb


class Rosters:
    files = settings.get_data_file_paths()
    # players = list(pd.read_csv(files["rosters.csv"], usecols=["person_id"])["person_id"].unique())

    def all_rosters_to_df(self):
        if "rosters.csv" in self.files:
            return pd.read_csv(self.files["rosters.csv"])
        
        teams_df = ndr.get_teams()
        rosters_all = [self.get_team_roster_all_seasons(t[0], t[1]) 
                       for t in teams_df[["id", "firstYearOfPlay"]].itertuples(index=False)]
        rosters_df = pd.concat(rosters_all)
        
        settings.write_df_to_csv(df=rosters_df, fp="data/rosters", fn="rosters.csv")
        return rosters_df

    def all_player_metadata_to_df(self):
        if "metadata.csv" in self.files:
            return pd.read_csv(self.files["metadata.csv"])
        if "rosters.csv" in self.files:
            players = list(pd.read_csv(self.files["rosters.csv"], usecols=["person_id"])["person_id"].unique())
        else:
            players = list(self.all_rosters_to_df()["person_id"].unique())

        n = len(players)
        player_metadata = [(pb.report(i, n), self.get_player_metadata(player))[1]
                           for i, player in enumerate(players)]

        player_metadata_df = pd.concat(player_metadata)
        settings.write_df_to_csv(df=player_metadata_df, fp="data/metadata", fn="metadata.csv")
        return player_metadata_df

    def get_team_roster(self, team, season):
        ss = ndr.season_string(season)
        url = f"https://statsapi.web.nhl.com/api/v1/teams/{team}?expand=team.roster&season={ss}"
        body = ndr.get_request(url)
        if body:
            try:
                df = pd.json_normalize(body["teams"][0]["roster"]["roster"], sep="_")
                df["season"] = ss
                df["team_id"] = team
                return df
            except Exception as e:
                print(e)
                return None
        else:
            return None

    def get_team_roster_all_seasons(self, team, firstSeason):
        season_year = ndr.get_current_season_year()
        roster_list = []
        for season in range(int(firstSeason), season_year):
            roster = self.get_team_roster(team, season + 1)
            if roster is not None:
                roster_list.append(roster)
        return pd.concat(roster_list)

    def get_player_metadata(self, player_id):
        url = f"https://statsapi.web.nhl.com/api/v1/people/{player_id}"
        body = ndr.get_request(url)
        if body:
            return pd.json_normalize(body["people"], sep="_")
        else:
            return None
