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


def cities(city_url, gapi_key):
    # create blank dataframe
    cities_df = pd.DataFrame(
        columns=[
            "id",
            "href",
            "name",
            "feature",
            "country",
            "state",
            "latitude",
            "longitude",
            "population",
        ]
    )

    # headers
    headers = {"X-Application-Key": gapi_key, "Accept-Language": "en"}

    last_page = False
    city_url = city_url

    while last_page == False:

        data = requests_retry_session().get(city_url, headers=headers,).json()

        current_cities = pd.DataFrame.from_dict(data["results"])
        cities_df = cities_df.append(current_cities, sort=False)
        print("added {} cities for {}".format(len(current_cities.index), city_url))

        for link in data["links"]:
            if link["rel"] == "next":
                city_url = link["href"]
                updated = True
            else:
                pass

        # if the link wasn't updated, it's the last page
        if updated != True:
            last_page = True
        else:
            updated = False

    # get total number of cities
    total_cities = len(cities_df.index)

    # extract the country and state information
    cities_df["country_ISO"] = cities_df.apply(lambda row: row.country["id"], axis=1)
    cities_df["state_name"] = cities_df.apply(lambda row: row.state["name"], axis=1)

    # drop unnecessary columns
    cities_df = cities_df.drop(
        columns=["href", "population", "feature", "country", "state"]
    )

    return cities_df

