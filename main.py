import requests
import pandas as pd
import datetime
import threading
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


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


# set application key
from secret import weather_api_key, gapi_key

# import module to get all cities that are relevant
from get_cities import cities
from weather_api import get_monthly_weather

# first get all locations that we need historical weather for
city_url = "https://rest.gadventures.com/places/?feature.code=PPL&country.name=Australia&state.id=AU-TAS"

cities_df = cities(city_url, gapi_key)

print(cities_df)

# for each location, make an api call, and append the relevant details to the dataframe

cities_df = cities_df[0:3]

# save a blank dictionary so we can add to it, city-by-city
climateAverage = {}

for city in cities_df.iterrows():
    data = get_monthly_weather(
        weather_api_key, city[1]["latitude"], city[1]["longitude"]
    )

    climateAverage[city[1]["id"]] = data["data"]["ClimateAverages"]


print(climateAverage)
