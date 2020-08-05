import requests
import pandas as pd
import datetime
import threading
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from main.py import columns


def requests_retry_session(
    retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None,
):
    # have we created one in this thread?
    my_local_data = threading.local()
    if hasattr(my_local_data, "http_session"):
        return my_local_data.http_session
    # else create a new one
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    # save it for future use
    my_local_data.http_session = session
    return session


def get_monthly_weather(weather_api_key, city, latitude, longitude):

    url = "https://api.worldweatheronline.com/premium/v1/weather.ashx?key={}&q={},{}&format=json&num_of_days=0&fx=no&cc=no&mca=yes&fx24=no&includelocation=no&show_comments=no".format(
        weather_api_key, latitude, longitude
    )

    data = requests_retry_session().get(url).json()

    # save base level of returned data
    weather = data["data"]["ClimateAverages"][0]["month"]

    # take each variable and add it to the relevant list in the columns dictionary
    columns["id"].append(city[1]["id"])
    columns["name"].append(city[1]["name"])
    columns["state"].append(city[1]["state_name"])
    columns["country"].append(city[1]["country_ISO"])
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

    print("Completed successfully for {}".format(city[1]["name"]))

    return
