import requests
import json
import os
import boto3

key = "OnFCwJszGl9TrwnMtkRYVZPdq3lGyFs5kYbj2vKM"



def import_split():
    url_schedule = f"https://api.sportradar.com/nba/trial/v8/en/seasons/2023/REG/teams/583ec825-fb46-11e1-82cb-f4ce4684ea4c/splits/schedule.json?api_key={key}"
    headers = {"accept": "application/json"}
    response = requests.get(url_schedule, headers=headers)
    return response.json()

res = import_split()

def extract_player(res, player_id):
    players = res["players"]
    for player in players:
        if player["id"] == player_id:
            curry_splits = player["splits"]
    for split in curry_splits:
        print(split["category"])
extract_player(res, "8ec91366-faea-4196-bbfd-b8fab7434795")
        
           
   




