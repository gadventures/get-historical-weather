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


def get_monthly_weather(weather_api_key, latitude, longitude):

    url = "https://api.worldweatheronline.com/premium/v1/weather.ashx?key={}&q={},{}&format=json&num_of_days=0&fx=no&cc=no&mca=yes&fx24=no&includelocation=no&show_comments=no".format(
        weather_api_key, latitude, longitude
    )

    data = requests_retry_session().get(url).json()

    return data
