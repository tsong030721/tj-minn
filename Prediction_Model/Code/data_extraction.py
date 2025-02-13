# File for extracting player stats using nba api
import api_setup
from pathlib import Path
from nba_api.stats.endpoints.playergamelogs import PlayerGameLogs as Player

# Convert year to season
def year_to_season(year):
    end = str((year + 1))[-2:]
    season = str(year) + "-" + end
    return season

# Season year in full (ex. 2002), returns logs or records
def logs_season(year, record):
    if year < 1950 or year > 2023:
        print("No data for year %d.", year)
        return None
    
    season = year_to_season(year)
    logs = Player(season_nullable = season)
    df = logs.get_data_frames()[0]
    if not record:
        return df
    
    directory = Path("Prediction_Model/Data")
    file = season + ".parquet"
    if not (directory / file).exists():
        file_path = "../Data/" + file
        df.to_parquet(file_path, index = False)
    
    return None

# Returns logs (in a list) or records them for a range of seasons
def logs_seasons(start, end, record):
    if start > end or start > 2023 or end < 1950:
        print("Invalid range of seasons.")
        return None

    year = max(1950, start)
    end = min(2023, end)
    res = []
    if record:
        res = None

    while year <= end:
        season = year_to_season(year)
        logs = Player(season_nullable = season)
        df = logs.get_data_frames()[0]

        if not record:
            res.append(df)
        else:
            directory = Path("Prediction_Model/Data")
            file = season + ".parquet"
            if not (directory / file).exists():
                file_path = "../Data/" + file
                df.to_parquet(file_path, index = False)

        year += 1
    
    return res

if __name__ == "__main__":
    print("Hello World")
