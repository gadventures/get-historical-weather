from queue import Queue, Empty
import requests
import pandas as pd
import datetime
import time
import threading
from functools import partial
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# local module imports

# set application key
from secret import weather_api_key, gapi_key

# import utils
from utils import *

# create a dictionary composed of blank lists. Here is where we will put the information which becomes dataframe columns.

columns = {
    "id": [],
    "name": [],
    "state": [],
    "country": [],
    "Jan": [],
    "Feb": [],
    "Mar": [],
    "Apr": [],
    "May": [],
    "Jun": [],
    "Jul": [],
    "Aug": [],
    "Sep": [],
    "Oct": [],
    "Nov": [],
    "Dec": [],
}

# create function that gets place information by ID, and write to the dataframe
def get_weather_by_id(city_id, gapi_key, weather_api_key, city_ids):

    global counter
    global columns

    # headers
    headers = {"X-Application-Key": gapi_key, "Accept-Language": "en"}

    data = (
        requests_retry_session()
        .get("https://rest.gadventures.com/places/{}".format(city_id), headers=headers,)
        .json()
    )

    name = data["name"]
    if data["state"] is not None:
        state = data["state"]["name"]
    else:
        state = ""
    latitude = data["latitude"]
    longitude = data["longitude"]
    country = data["country"]["name"]

    url = "https://api.worldweatheronline.com/premium/v1/weather.ashx?key={}&q={},{}&format=json&num_of_days=0&fx=no&cc=no&mca=yes&fx24=no&includelocation=no&show_comments=no".format(
        weather_api_key, latitude, longitude
    )

    data = requests_retry_session().get(url).json()

    # save base level of returned data
    weather = data["data"]["ClimateAverages"][0]["month"]

    # take each variable and add it to the relevant list in the columns dictionary
    columns["id"].append(city_id)
    columns["name"].append(name)
    columns["state"].append(state)
    columns["country"].append(country)
    columns["Jan"].append(weather[0])
    columns["Feb"].append(weather[1])
    columns["Mar"].append(weather[2])
    columns["Apr"].append(weather[3])
    columns["May"].append(weather[4])
    columns["Jun"].append(weather[5])
    columns["Jul"].append(weather[6])
    columns["Aug"].append(weather[7])
    columns["Sep"].append(weather[8])
    columns["Oct"].append(weather[9])
    columns["Nov"].append(weather[10])
    columns["Dec"].append(weather[11])

    print("added {}, number {} of {}".format(name, counter, len(city_ids.index)))

    counter += 1

    return


def main():

    # import list of city IDs that we need

    city_ids = pd.read_csv("places_list.csv")

    # city_ids = city_ids[0:24]

    # for each location, make an api call, and append the relevant details to the dataframe
    global counter
    counter = 0
    tasks = []

    for city in city_ids.iterrows():
        task = partial(
            get_weather_by_id, city[1]["place_id"], gapi_key, weather_api_key, city_ids
        )
        tasks.append(task)

    # now call runBatch from the workers file above
    tic = time.perf_counter()
    runBatch(tasks, num_worker_threads=12)
    toc = time.perf_counter()
    total_time = toc - tic
    print(total_time)
    print("Gathering of weather completed.")

    # cities_df = cities_df[0:3]

    climate_df = pd.DataFrame(data=columns)
    # change "id" column to be the index
    climate_df.set_index("id", inplace=True)

    # take each monthly column and spread the data into invidual columns, with the month as the prefix
    month_names = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]

    for month in month_names:
        avgMinTempC = "{}_avgMinTempC".format(month)
        avgMinTempF = "{}_avgMinTempF".format(month)
        avgMaxTempC = "{}_avgMaxTempC".format(month)
        avgMaxTempF = "{}_avgMaxTempF".format(month)
        avgDailyRainfall = "{}_avgDailyRainfall".format(month)

        climate_df[avgMinTempC] = climate_df.apply(
            lambda row: "{:.2f}".format(float(row[month]["avgMinTemp"])), axis=1
        )
        climate_df[avgMaxTempC] = climate_df.apply(
            lambda row: "{:.2f}".format(float(row[month]["absMaxTemp"])), axis=1
        )
        climate_df[avgMinTempF] = climate_df.apply(
            lambda row: "{:.2f}".format(float(row[month]["avgMinTemp_F"])), axis=1
        )
        climate_df[avgMaxTempF] = climate_df.apply(
            lambda row: "{:.2f}".format(float(row[month]["absMaxTemp_F"])), axis=1
        )
        climate_df[avgDailyRainfall] = climate_df.apply(
            lambda row: "{:.2f}".format(float(row[month]["avgDailyRainfall"])), axis=1
        )

    climate_df.drop(columns=month_names, inplace=True)

    climate_df.to_csv("climate_data.csv")


if __name__ == "__main__":
    main()
