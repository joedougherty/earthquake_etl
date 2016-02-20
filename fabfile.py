# System-level modules
from fabric.api import task, env, local
from tinyetl import TinyETL
import os
import requests
import sqlite3
import pandas as pd
import iso8601

# Helper script
from eq_database import create_eq_table, get_db

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
    # Non-required args, but good for namespacing purposes
    db_location = os.path.join(os.getcwd(), 'database', 'earthquakes.sqlite'),
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
    all_earthquakes = os.path.join(os.getcwd(), 'download_data', 'all_earthquakes', 'all_month.csv')
    create_eq_table(all_earthquakes, etl.db_location)

@task
def info():
    """ Print some useful information about this ETL process. """
    setup()
    print(etl)

@etl.log
def setup():
    """ This is a good place for any runtime additions to the etl object. """
    etl.csv_file_location = os.path.join(etl.tmpdata_dir, 'eq_{}.csv'.format(etl.timestamp()))
    etl.db = get_db(etl.db_location)

@etl.log
def update_data_is_newer(latest_prod_date, earliest_update_date):
    latest_production_time = iso8601.parse_date(latest_prod_date)
    earliest_new_data_time = iso8601.parse_date(earliest_update_date)
    
    if earliest_new_data_time > latest_production_time:
        return True
    return False

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
@etl.log
def append_newest_data():
    """ Parse the newest data from USGS and append it to the final dataset. """
    df = pd.read_csv(etl.csv_file_location)
    num_new_rows = len(df)

    earliest_new_data_time = df.time.min()
    latest_prod_date_result = etl.db.cursor.execute('select max(time) from all_earthquakes')
    latest_prod_date = latest_prod_date_result.fetchone()[0]

    # Verify that the downloaded records are actually new 
    # with respect to the previously existing data
    if num_new_rows > 0 and update_data_is_newer(latest_prod_date, earliest_new_data_time):
        new_records = [tuple(x) for x in df.to_records(index=False)]
        insert_query = "INSERT INTO all_earthquakes VALUES" \
                       "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        # Write new rows to the database
        etl.db.cursor.executemany(insert_query, new_records) 
        etl.logger.info("Wrote {} rows to database.".format(num_new_rows))
    else:
        etl.logger.info("{} contains no new rows. Process ends here.".format(etl.csv_file_location))

@task()
def main():
    """ Run the whole ETL process. """
    setup()
    download_data()
    append_newest_data()
   
