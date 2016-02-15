from collections import namedtuple
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

def create_eq_table(db_location):
    """ 
    Create the initial table for the earthquake data to be written to.
    db_location should be an absolute path.
    """
    fields_and_types = [
        ('date', 'TEXT'), ('latitude', 'REAL'), 
        ('longitude', 'REAL'), ('depth', 'REAL'), 
        ('mag', 'REAL'), ('magType', 'TEXT'), 
        ('nst', 'INT'), ('gap', 'REAL'), 
        ('dmin', 'REAL'), ('rms', 'REAL'), 
        ('net', 'TEXT'), ('id', 'TEXT'), 
        ('updated', 'TEXT'), ('place', 'TEXT'), 
        ('type', 'TEXT'), ('horizontalError', 'REAL'), 
        ('depthError', 'REAL'), ('magError', 'REAL'), 
        ('magNst', 'INT'), ('status', 'TEXT'), 
        ('locationSource', 'TEXT'), ('magSource', 'TEXT')]
    
    values = ""
    for field_and_type in fields_and_types:
        values += "{} {} ".format(field_and_type[0], field_and_type[1])

    db = get_db_connection(db_location)
    db.cursor.execute("CREATE TABLE earthquakes ({});".format(values))
    db.conn.commit()
    db.conn.close()
