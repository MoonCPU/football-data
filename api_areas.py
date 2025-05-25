import os
import requests
from dotenv import load_dotenv
import json

load_dotenv()

API_KEY = os.getenv('FOOTBALL_API_KEY')

if not API_KEY:
    raise ValueError("API key not found. Check the .env file again.")

url = "https://api.football-data.org/v4/areas/"
headers = {
    "X-Auth-Token": API_KEY
}

response = requests.get(url, headers=headers)
if response.status_code == 200:
    data = response.json()
    # save the response json into matches.json file
    with open("areas.json", "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    print("Data fetched and saved to areas.json")
else:
    print(f"Failed to fetch data: {response.status_code}")
    print(response.text)