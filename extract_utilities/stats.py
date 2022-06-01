import extract_utilities.nhl_data_requests as ndr
import pandas as pd

import settings
import progress_bar as pb


class Stats:
    files = settings.get_data_file_paths()
    if "metadata.csv" not in files:
        raise Exception("Please run roster metadata")
    player_metadata = pd.read_csv(files["metadata.csv"], usecols=["id", "primaryPosition_code"])
 
    def get_all_player_stats(self, player_type):
        if player_type not in ["skater", "goalie"]:
            raise Exception("Please specifiy skater or goalie stat type")
        if player_type == "skater":
            mask = ['C', 'L', 'D', 'R']
        if player_type == "goalie":
            mask = ['G']
        
        directory = settings.get_folder()
        temp_directory = settings.get_folder(new_folder=player_type)

        players = list(self.player_metadata.loc[self.player_metadata["primaryPosition_code"].isin(mask)]["id"].unique())
        n = len(players)

        for i, player in enumerate(players):
            player_stats = (pb.report(i, n), self.get_player_stats(player))[1]
            player_stats.to_csv(f"{temp_directory}/{player}.csv", index=False)

        player_df = pd.concat([pd.read_csv(file)
                               for file in settings.wildcard_files(f"{temp_directory}/*.csv")])
        player_df.to_csv(f"{directory}/{player_type}s.csv", index=False)
        settings.remove_folder(temp_directory)

        return True
        
    def get_player_stats(self, player_id, stats_type="yearByYear"):
        url = f"https://statsapi.web.nhl.com/api/v1/people/{player_id}/stats?stats={stats_type}"
        body = ndr.get_request(url)

        stats = pd.concat([pd.json_normalize(b, sep="_") for b in body["stats"][0]["splits"]])
        stats.columns = stats.columns.str.replace("stat_", "")
        stats["player_id"] = player_id
        return stats
