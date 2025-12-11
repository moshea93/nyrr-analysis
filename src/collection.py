import requests
import json
import time
import os
import pandas as pd
import fastparquet

headers = {
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0",            
        "origin": "https://results.nyrr.org",   # good for anti-abuse
        "referer": "https://results.nyrr.org/", # good for anti-abuse
        "accept": "application/json"
    }


# Retry on failure
def exponential_backoff(url, headers, payload, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            return requests.post(url, headers=headers, json=payload, timeout=10)
        except requests.exceptions.ConnectionError as e:
            print(f"[Attempt {attempt}] Connection error: {e}")
        except requests.exceptions.Timeout:
            print(f"[Attempt {attempt}] Timeout error.")

        sleep_time = attempt ** 2
        print(f"Retrying in {sleep_time} seconds...")
        time.sleep(sleep_time)

    raise RuntimeError("Failed after retries")


# Get all NYRR races for a given year
def fetch_api_races(year):
    url = "https://rmsprodapi.nyrr.org/api/v2/events/search"

    payload = {
        "year":year,
        "sortColumn":"StartDateTime",
        "sortDescending":1,
        "pageIndex":1,
        "pageSize":100
    }

    response = exponential_backoff(url, headers, payload)
    response.raise_for_status()
    return response.json()


# Get all NYRR races from 1970 - present
def get_all_races():
    output_dir = "../data/raw/races"
    os.makedirs(output_dir, exist_ok=True)

    year = 2025

    # Create a JSON file of races for each year
    while year >= 1970:
        print(f"Fetching {year} races...")

        response = fetch_api_races(year)
        results = response['items']

        # Write to JSON
        with open(f"{output_dir}/{year}.json", "w") as f:
            json.dump(results, f, indent=4)

        year -= 1

        time.sleep(.5) # a little relief for the API


# Take the JSON race files and make a single CSV
def write_races_to_csv():
    race_list = []

    # Collect all JSON files with race details
    race_dir = "../data/raw/races"
    files = [entry.name for entry in os.scandir(race_dir) if entry.is_file()]

    # Make one big list of json objects, one for each race
    for file in files:
        with open(f"{race_dir}/{file}", "r") as f:  
            file_race_list = json.load(f)
        race_list.extend(file_race_list)

    # Make a clean dataframe
    df = pd.DataFrame(race_list)
    df['year'] = pd.to_datetime(df['startDateTime']).dt.year # Will store race_results by year later in script
    df['eventName'] = df['eventName'].str.replace("/", "-") # Will be storing race results by eventName, removes file vs folder writing issues
    df = df.sort_values(by="startDateTime", ascending=False)

    # Write to CSV
    os.makedirs("../data/clean", exist_ok=True)
    df.to_csv("../data/clean/races.csv", index=False)


# Collect 100 race results at a time
def fetch_api_race_results(url, event_code="25TC15K", highest_place_collected = 0):
   
   # Top 100 finishers that we haven't already gotten
    payload = {
        "overallPlaceFrom": highest_place_collected + 1,
        "overallPlaceTo": highest_place_collected + 100,
        "eventCode": event_code,
        "sortColumn": "overallTime",
        "sortDescending": False,
        "pageIndex": 1,
        "pageSize": 100 # max page size allowed by API
    }

    response = exponential_backoff(url, headers, payload)
    response.raise_for_status()
    return response.json()


# Collect total finisher count for a race
def fetch_api_finisher_count(url, event_code):
    payload = {
        "eventCode":event_code,
        "pageSize":1
    }

    response = exponential_backoff(url, headers, payload)
    response.raise_for_status()
    return response.json()['totalItems']


def get_race_results(event_code):
    results = []
    highest_place_collected = 0

    url = "https://rmsprodapi.nyrr.org/api/v2/runners/finishers-filter"

    total_finishers = fetch_api_finisher_count(url, event_code)
    print(f"Total finishers: {total_finishers}")

    # Keep making calls until we've collected the last placed finisher
    while True:
        response = fetch_api_race_results(url, event_code, highest_place_collected)
        new_results = response['items']

        if len(new_results) == 0:
            break

        # Add new batch of results to overall results
        results.extend(new_results)
        highest_place_collected = results[-1]["overallPlace"]

        print(f"{len(results)} results collected")

        time.sleep(.5) # relief for the API

    return results


def get_all_race_results():
    race_df = pd.read_csv("../data/clean/races.csv")

    # Store races for which we've already collected results, in previous passes
    race_dir = "../data/raw/race_results"
    event_results_collected = [os.path.splitext(name)[0] for root, dirs, filenames in os.walk(race_dir) for name in filenames]

    # Iterate through races
    for index, row in race_df.iterrows():
        event_name = row["eventName"]
        event_code = row["eventCode"]
        year = row["year"]

        file_name = f"{event_name} ({event_code})"

        # Skip if we've already collected results for the race
        if file_name in event_results_collected:
            print(f"Already collected {file_name} results")
            continue

        if file_name == "Grete's Great Gallop (Grete's)":
            print(f"No results available for {file_name}")
            continue

        # Collect results
        print(f"\nGetting {file_name} results...")
        event_results = get_race_results(event_code)

        # Write to JSON
        os.makedirs(f"{race_dir}/{year}", exist_ok=True)
        with open(f"{race_dir}/{year}/{file_name}.json", "w") as f:
            json.dump(event_results, f, indent=4)


# # Take the JSON race result files and make a single CSV
def write_results_to_csv():
    results_list = []

    # Collect all JSON files with race details
    race_dir = "../data/raw/race_results"
    race_paths = [os.path.join(root, name) for root, dirs, filenames in os.walk(race_dir) for name in filenames]
    race_paths = [race_path.removeprefix(f"{race_dir}/") for race_path in race_paths]

    # Make one big list of json objects, one for each race

    race_count = 0

    for race_path in race_paths:
        with open(f"{race_dir}/{race_path}", "r") as f:  
            file_results_list = json.load(f)

        # Put event details at front of each result
        event_year = int(race_path.split("/")[0])
        event_name = race_path[race_path.find("/")+1:race_path.find(" (")] 
        event_code = race_path[race_path.find("(")+1:race_path.find(")")]
        file_results_list = [
            {
                'eventCode': event_code,
                'eventYear': event_year,
                'eventName': event_name,
                **{k: v for k, v in result.items()}
            }
            for result in file_results_list]
    
        results_list.extend(file_results_list)

        race_count += 1
        print(f"Race count: {race_count}, results: {len(results_list)}")

    # Make a clean dataframe
    df = pd.DataFrame(results_list)
    df = df.sort_values(by=["eventYear", "eventCode", "overallPlace"])

    # Write
    os.makedirs("../data/clean", exist_ok=True)
    # df.to_csv("../data/clean/race_results.csv", index=False)
    df.to_parquet("../data/clean/race_results.parquet", index=False, engine="fastparquet")

# get_all_races()
# write_races_to_csv()
# get_all_race_results()
# write_results_to_csv()




#### SCRATCH

# # Event results curl
# curl 'https://rmsprodapi.nyrr.org/api/v2/runners/finishers-filter' \
#   -H 'content-type: application/json' \
#   -H 'user-agent: Mozilla/5.0' \
#   -H 'origin: https://results.nyrr.org' \
#   -H 'referer: https://results.nyrr.org/' \
#   --data '{"eventCode":"25TC15K",
#            "sortColumn":"overallTime",
#            "sortDescending":false,
#            "pageIndex":1,
#            "pageSize":1}'

# # Events Curl
# curl 'https://rmsprodapi.nyrr.org/api/v2/events/search' \
#   -H 'content-type: application/json;charset=UTF-8' \
#   -H 'origin: https://results.nyrr.org' \
#   -H 'referer: https://results.nyrr.org/' \
#   -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36' \
#   --data-raw '{"searchString":null,"distance":null,"year":null,"notOlderDays":null,"sortColumn":"StartDateTime","sortDescending":1,"pageIndex":1,"pageSize":51}'

df = pd.read_parquet("../data/clean/race_results.parquet")
print(df.head())