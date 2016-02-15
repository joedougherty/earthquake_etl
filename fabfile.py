from fabric.api import task, env, local
from tinyetl import TinyETL
import os
import requests
import sqlite3
from eq_database import create_eq_table 

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

#-----------------------------#
# Helper Tasks/Functions      #
#-----------------------------#
@task
def create_required_directories():
    """ Create directories for logs and tmpdata to live in. """
    local("mkdir -p {}".format(etl.log_dir))
    local("mkdir -p {}".format(etl.tmpdata_dir))

@task
def create_initial_database():
    """ Create initial database for real-time earthquake data. """
    if not etl.dry_run:
        etl.database_dir = os.path.join(os.getcwd(), 'database') 
        create_eq_table(os.path.join(etl.database_dir, 'earthquakes.sqlite'))

@etl.log
def set_csv_file_location():
    if not etl.dry_run:
        etl.csv_location = os.path.join(etl.tmpdata_dir, 'eq_{}.csv'.format(etl.timestamp()))

#-----------------------------#
# ETL Tasks                   #
#-----------------------------#
@task
@etl.log
def download_data():
    """ Download latest data from http://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php """
    endpoint = "http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.csv"
    etl.download_file(endpoint, etl.csv_file_location)

@task
def main():
    """ Run the whole ETL process. """
    set_csv_file_location()
    download_data()

