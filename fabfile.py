# System-level modules
from fabric.api import task, env, local, lcd
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

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

etl = TinyETL(
    'eq_intraday',
    description,
    env=env, 
    log_dir=os.path.join(BASE_DIR, "logs"),
    tmpdata_dir=os.path.join(BASE_DIR, "download_data"),
    # Non-required args, but good for namespacing purposes
    db_location = os.path.join(BASE_DIR, 'database', 'earthquakes.sqlite'),
)

#-----------------------------#
# Helper Tasks/Functions      #
#-----------------------------#
@task
def info():
    """ Print some useful information about this ETL process. """
    print(etl)

@task
def view_last_log():
    """ Shortcut to read last log. """
    with lcd(etl.log_dir):
        filename = local("ls -alrt *.log | awk '{print $9}' | tail -1", capture=True)
        local("less {}".format(filename))

@etl.log
def setup():
    """ This is a good place for any runtime additions to the etl object. """
    etl.csv_file_location = os.path.join(etl.tmpdata_dir, 'eq_{}.csv'.format(etl.timestamp()))
    etl.db = get_db(etl.db_location)

    # This could also be used as a place to output
    # all etl object attrs to the log for debugging purposes
    # Example: 
    etl.logger.debug(etl)
    
@etl.log
def update_data_is_newer(latest_prod_date, earliest_update_date):
    latest_production_time = iso8601.parse_date(latest_prod_date)
    earliest_new_data_time = iso8601.parse_date(earliest_update_date)
    
    if earliest_new_data_time > latest_production_time:
        return True
    return False

@etl.log
def write_new_records_to_db(new_records):
    insert_query = "INSERT INTO all_earthquakes VALUES" \
                   "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    etl.db.cursor.executemany(insert_query, new_records) 
    etl.db.conn.commit()

    etl.logger.info("Wrote {} rows to database.".format(len(new_records)))
    
#-----------------------------#
# ETL Tasks                   #
#-----------------------------#
@task
@etl.log
def download_data():
    """ Download latest data from http://earthquake.usgs.gov/earthquakes/feed/v1.0/csv.php """
    endpoint = "http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.csv"
    etl.logger.info("Downloaded data for this run: {}".format(etl.csv_file_location))
    etl.download_file(endpoint, etl.csv_file_location)

@task
@etl.log
def append_newest_data():
    """ Parse the newest data from USGS and append it to the final dataset. """
    df = pd.read_csv(etl.csv_file_location)
    num_new_rows = len(df)

    earliest_new_update_date = df.time.min()
    latest_prod_date_result = etl.db.cursor.execute('select max(time) from all_earthquakes')
    latest_prod_date = latest_prod_date_result.fetchone()[0]

    # Log the pertinent dates
    etl.logger.info("Latest production data date: {}".format(latest_prod_date))
    etl.logger.info("Earliest update data date: {}".format(earliest_new_update_date))

    # Verify that the downloaded records are actually new 
    # with respect to the previously existing data
    if num_new_rows > 0 and update_data_is_newer(latest_prod_date, earliest_new_update_date):
        write_new_records_to_db( [tuple(x) for x in df.to_records(index=False)] )
    else:
        msg = "{} contains no new rows. Process ends here.".format(etl.csv_file_location)
        print(msg)
        etl.logger.info(msg)

@task
def main():
    """ Run the whole ETL process. """
    setup()
    download_data()
    append_newest_data()
   
