import requests

import pandas as pd


def get_request(url):
    import random
    import sys
    sleep_time = 0
    while True:
        try:
            response = requests.get(url)
            break
        except Exception as e:
            print(e)
            rand = random.randint(0, 10)
            sys.sleep(rand)
            sleep_time += rand
            if sleep_time > 60:
                return None
    if response.status_code == 200:
        try:
            return response.json()
        except Exception as e:
            print(e)
            return None
    

def season_string(season):
    previous_season = int(season) - 1
    return f"{previous_season}{season}"


def _get_current_season():
    url = "https://statsapi.web.nhl.com/api/v1/seasons/current"
    r = requests.get(url)
    return r.json()["seasons"][0]


def get_current_season_year():
    return int(_get_current_season()["seasonEndDate"].split("-")[0])


def get_teams():
    url = "https://statsapi.web.nhl.com/api/v1/teams"
    body = get_request(url)
    return pd.concat([pd.json_normalize(b, sep="_") for b in body["teams"]])