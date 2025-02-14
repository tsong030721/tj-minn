import requests
import json
import os
import boto3
#s3
s3 = boto3.client("s3")

#sportradar API key
key = os.environ["SPORTRADAR_API_KEY"]

#This imports for a specific team.
def import_split(team_id):
    url_schedule = f"https://api.sportradar.com/nba/trial/v8/en/seasons/2023/REG/teams/{team_id}/splits/schedule.json?api_key={key}"
    headers = {"accept": "application/json"}
    response = requests.get(url_schedule, headers=headers)
    return response.json()

res = import_split(team_id = "583ec825-fb46-11e1-82cb-f4ce4684ea4c")

def extract_player(res, player_id):
    players = res["players"]
    for player in players:
        #search player
        if player["id"] == player_id:
            #This imports splits for each player
            player_splits = player["splits"]
    
    for split in player_splits:
        if split["category"] == "last_10":
            last_10 = split["total"]
      
    return last_10
data = extract_player(res, player_id= "8ec91366-faea-4196-bbfd-b8fab7434795")
print(data)

def store_data_in_s3(data):
    bucket_name = os.environ["S3BUCKET"]
    file_name = "nba_test_curry.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data, indent=4),
        ContentType="application/json"
    )

    print(f"Cleaned data saved to {bucket_name}/{file_name}")
def lambda_handler(event, context):
    """Main AWS Lambda function handler."""
    nba_data = extract_player(res, player_id= "8ec91366-faea-4196-bbfd-b8fab7434795")
    if nba_data:
        cleaned_data = nba_data  # Extract relevant fields
        store_data_in_s3(cleaned_data)  # Upload to S3
        return {"status": "success", "message": "Cleaned data stored in S3"}
    else:
        return {"status": "error", "message": "Failed to fetch NBA data"}






