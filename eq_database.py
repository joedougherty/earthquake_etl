from collections import namedtuple
import pandas as pd
import sqlite3

""" 
Data types and any add'l metadata from: 
http://earthquake.usgs.gov/earthquakes/feed/v1.0/glossary.php 
"""

def get_db_connection(db_name):
    """ 
    Create a DB connection to the table passed in by db_name. 
    Returns a namedtuple containing the connection object and the DB cursor.
    """
    db_conn_mgr = namedtuple('db_conn_mgr', ['conn', 'cursor'])
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    return db_conn_mgr(conn, cursor)

def create_eq_table(csv_file, db_location):
    dataframe = pd.read_csv(csv_file)
    db = get_db_connection(db_location)
    dataframe.to_sql('all_earthquakes', db.conn)
