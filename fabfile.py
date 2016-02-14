from fabric.api import task, env
from tinyetl import TinyETL
import os
import requests

description = """
Real-time Earthquake Data 
Source: http://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php
"""

etl = TinyETL(
    'eq_intraday',
    description,
    env=env, 
    log_dir=os.path.join(os.getcwd(), "logs"),
    tmpdata_dir=os.path.join(os.getcwd(), "download_data"),
)

@task
@etl.log
def download_data():
    """ Download latest data from http://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php """

    endpoint = "http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.csv"
    csv_file = os.path.join(etl.tmpdata_dir, 'eq_{}.csv'.format(etl.timestamp()))

    etl.download_file(endpoint, csv_file)

@task
# Note the absence of `@etl.log` here
def main():
    # download_data()
    pass

