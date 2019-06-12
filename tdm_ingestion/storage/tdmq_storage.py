import logging
import os
from typing import List

import requests
from tdm_ingestion.ingestion import Storage, TimeSeries


class TDMQStorage(Storage):
    def __init__(self, tdmq_url: str):
        self.tdmq_url = tdmq_url

    def write(self, timeseries: List[TimeSeries]):
        if  timeseries:
            r = requests.post(os.path.join(self.tdmq_url, 'api/v0.0/measures'),
                          json=[ts.to_dict() for ts in timeseries])
            logging.debug(r.content)
            r.raise_for_status()
