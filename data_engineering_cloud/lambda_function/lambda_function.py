import requests
import json
import os
import boto3
#s3
s3 = boto3.client("s3")

#sportradar API key
key = os.environ["SPORTRADAR_API_KEY"]

def get_teams():
    #API tripping so I had to hard code this bit
    NBA_teams=['76ers','Bucks','Bulls','Cavaliers', 'Celtics', 'Clippers','Grizzlies','Hawks', 'Heat', 'Hornets',
               'Jazz', 'Kings', 'Kings', 'Knicks', 'Lakers',
               'Magic', 'Mavericks', 'Nets', 'Nuggets', 'Pelicans', 'Pistons','Raptors'
               ,'Rockets', 'Spurs', 'Suns', 'Thunder', 'Timberwolves', 'Trail Blazers', 'Warriors',  'Wizards']
    teamid_list = []
    url = f"https://api.sportradar.com/nba/trial/v8/en/league/teams.json?api_key={key}"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    res_json = response.json()
    print(res_json)
    d = res_json["teams"]
    for team_dict in d:
        if team_dict["name"] in NBA_teams:
            #append all ids
            teamid_list.append(team_dict["id"])
    return teamid_list
# teamids = get_teams()
        
# print(teamids)

#This imports for a specific team.
def import_split(team_id):
    url_schedule = f"https://api.sportradar.com/nba/trial/v8/en/seasons/2023/REG/teams/{team_id}/splits/schedule.json?api_key={key}"
    headers = {"accept": "application/json"}
    response = requests.get(url_schedule, headers=headers)
    return response.json()


#returns a dictionary of a given team split json and gives a dictionary of each player's last 10 game
#split. For instance, we give it a json file of GSW split, it will create a dictionary of 
# curry's id: his stats in jsonn file.
def extract_player_last_10(team_split):
    #collects json files
    collection = {}
    players = team_split.get("players")
    if not players:
        print("No players found in team_split:", team_split)
        return {}
    #access players from json from each team's json
    print(team_split)
    
    for player in players:
        #access each player's splits
        player_splits = player["splits"]
        for split in player_splits:
            if split["category"] == "last_10":
                last_10 = split["total"]
                collection[player["id"]] = last_10
    print(collection)
    return collection

# data = extract_player_last_10(res_for_team, player_id= "8ec91366-faea-4196-bbfd-b8fab7434795")

def store_data_in_s3(data, team_id, player_id):
    bucket_name = os.environ["S3BUCKET"]
    #player id
    file_name = f"last_10_team_{team_id}_player_{player_id}.json"
    data["team_id"] = team_id
    data["player_id"] = player_id
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Cleaned data saved to {bucket_name}/{file_name}")

def lambda_handler(event, context):
    """Main AWS Lambda function handler."""
    teamids = get_teams()
    count = 0
    for team_id in teamids:
        res_for_team = import_split(team_id)
        if 'message' in res_for_team and res_for_team['message'] == 'Object not found':
            print(f"Could not find object for the team with id {team_id}")
            continue
        if "players" not in res_for_team:
            print(f"'players' missing from team split for {team_id}: {res_for_team}")
            continue
        nba_data = extract_player_last_10(res_for_team)
        
        for player_id in nba_data:
            data = nba_data[player_id]
            if data:
                cleaned_data = data  # Extract relevant fields
                store_data_in_s3(cleaned_data, team_id, player_id)  # Upload to S3
                count +=1
    if count>0: 
        return {"status": "success", "message": f"Cleaned data ({count}) stored in S3"}
    else:
        return {"status": "error", "message": "Failed to fetch NBA data"}






