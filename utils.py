from queue import Queue, Empty
import requests
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
