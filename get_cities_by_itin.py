import requests
import pandas as pd
import datetime
import math
import csv
import threading
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# import utils
from utils import *
from secret import *


# headers
headers = {"X-Application-Key": gapi_key, "Accept-Language": "en"}

# make an API call to get all dossier codes in the API currently

# get number of pages required

data = (
    requests_retry_session()
    .get("https://rest.gadventures.com//tour_dossiers/", headers=headers,)
    .json()
)

number_of_pages = math.ceil(data["count"] / data["max_per_page"])
current_page = 0

# create a blank dataframe to hold the info

dossiers_df = pd.DataFrame(
    columns=[
        "id",
        "href",
        "name",
        "product_line",
        "departures_start_date",
        "departures_end_date",
    ]
)


# while current_page < (number_of_pages + 1):
while current_page < number_of_pages:

    # first call
    data = (
        requests_retry_session()
        .get(
            "https://rest.gadventures.com//tour_dossiers/?page={}".format(current_page),
            headers=headers,
        )
        .json()
    )

    current_dossiers = pd.DataFrame.from_dict(data["results"])
    dossiers_df = dossiers_df.append(current_dossiers, sort=False)

    current_page = current_page + 1


# get total number of dossiers
total_dossiers = len(dossiers_df.index)

# create a blank list of place IDs that we can later use to grab locations, and then weather
places_list = []

# get structured itinerary by dossier
counter = 1

for dossier in dossiers_df.iterrows():
    print("Attempting {} of 1250".format(counter))
    data = requests_retry_session().get(dossier[1]["href"], headers=headers,).json()
    structured_itin_url = data["structured_itineraries"][0]["href"]
    # get structured itin data
    itin_data = (
        requests_retry_session().get(structured_itin_url, headers=headers,).json()
    )
    for day in itin_data["days"]:
        # add the id of the start location to the places set if it's not already there
        if day["start_location"]["id"] not in places_list:
            places_list.append(day["start_location"]["id"])

    counter += 1
    df = pd.DataFrame(data={"place_id": places_list})
    df.to_csv("places_list.csv", sep=",", index=False)

print(len(places_list))

