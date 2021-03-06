#!/usr/bin/python
import psycopg2
from config import config, config_fiscal

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        conn = psycopg2.connect(**params)
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return psycopg2.connect(**params)

def connect_fiscal():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config_fiscal()
        conn = psycopg2.connect(**params)
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return psycopg2.connect(**params)