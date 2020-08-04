from queue import Queue, Empty
import requests
import pandas as pd
import datetime
import time
import threading
from functools import partial
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

# for each location, make an api call, and append the relevant details to the dataframe

cities_df = cities_df[0:3]

# create blank climate dataframe to write weather info to
climate_df = pd.DataFrame(
    columns=[
        "id",
        "name",
        "state",
        "country",
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
)

# create blank tasks list. A task is a city which will produce monthly weather, in a dataframe format

tasks = []


for city in cities_df.iterrows():
    task = partial(
        get_monthly_weather,
        weather_api_key,
        city,
        city[1]["latitude"],
        city[1]["longitude"],
    )
    tasks.append(task)


class Worker(threading.Thread):
    def __init__(self, queue, quit_ev, timeout=1):
        super(Worker, self).__init__()
        self.q = queue
        self.quit_ev = quit_ev
        self.timeout = timeout

    def run(self):
        while not self.quit_ev.is_set():
            try:
                item = self.q.get(True, self.timeout)  # block unless timeout
                if item is not None:
                    try:
                        item()
                    except Exception as err:
                        print(str(err))
                    self.q.task_done()
            except Empty:
                pass


def runBatch(listOfFunctions, num_worker_threads=4):
    quit_ev = threading.Event()
    q = Queue()
    kids = []
    for i in range(num_worker_threads):
        t = Worker(q, quit_ev)
        t.start()
        kids.append(t)

    for item in listOfFunctions:
        q.put(item)

    q.join()  # block until all tasks are done
    # signal child threads to end
    quit_ev.set()
    for t in kids:
        t.join()


# now call runBatch from the workers file above
tic = time.perf_counter()
runBatch(tasks, num_worker_threads=12)
toc = time.perf_counter()
print("Completed in {toc - tic:0.4f} seconds")

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
    climate_df[avgMinTempF] = climate_df.apply(
        lambda row: "{:.2f}".format(float(row[month]["avgMinTemp_F"])), axis=1
    )
    climate_df[avgMaxTempC] = climate_df.apply(
        lambda row: "{:.2f}".format(float(row[month]["absMaxTemp"])), axis=1
    )
    climate_df[avgMaxTempF] = climate_df.apply(
        lambda row: "{:.2f}".format(float(row[month]["absMaxTemp_F"])), axis=1
    )
    climate_df[avgDailyRainfall] = climate_df.apply(
        lambda row: "{:.2f}".format(float(row[month]["avgDailyRainfall"])), axis=1
    )

climate_df.drop(columns=month_names, inplace=True)

climate_df.to_csv("climate_data.csv")

