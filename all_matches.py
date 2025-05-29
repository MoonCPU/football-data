import requests
import json
import os

API_KEY = os.getenv("FOOTBALL_API_KEY")

if not API_KEY:
    raise ValueError("FOOTBALL_API_KEY environment variable not set.")

BASE_URL = "https://api.football-data.org/v4"
COMPETITION_IDS = [
    2021, # Premier League
    2014, # La Liga
    2002, # Bundesliga
    2019, # Serie A
    2015, # Ligue 1
    2001, # Champions League
]
SEASON_YEAR = 2024

all_matches_list = [] # This list will hold all match dictionaries

headers = {
    "X-Auth-Token": API_KEY
}

def fetch_competition_matches(competition_id, season, status="FINISHED"):
    url = f"{BASE_URL}/competitions/{competition_id}/matches"
    params = {
        "season": season,
        "status": status
    }
    print(f"Fetching matches for competition ID: {competition_id}, season: {season}...")
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched {len(data['matches'])} matches for competition {competition_id}.")
        # if matches field is empty, return empty list
        return data.get('matches', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for competition {competition_id}: {e}")
        return []

# for loop for every competition
for comp_id in COMPETITION_IDS:
    matches_from_competition = fetch_competition_matches(comp_id, SEASON_YEAR)
    if matches_from_competition: 
        all_matches_list.extend(matches_from_competition)

final_combined_data = {
    "matches": all_matches_list
}

output_filename = "all_matches_data.json"
with open(output_filename, "w", encoding="utf-8") as file:
    json.dump(final_combined_data, file, indent=4)

print(f"\nAll matches data from {len(COMPETITION_IDS)} competitions saved to {output_filename}")
print(f"Total matches collected: {len(all_matches_list)}")