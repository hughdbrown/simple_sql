#!/usr/bin/env python
from __future__ import print_function

import sqlite3 as lite
import pandas as pd


DATABASE_NAME = 'getting_started.db'


def create_cities_table(cur):
    try:
        cur.execute("DROP TABLE cities;")
    except:
        pass
    cur.execute("CREATE TABLE cities (name text, state text);")


def create_weather_table(cur):
    try:
        cur.execute("DROP TABLE weather;")
    except:
        pass
    cur.execute("CREATE TABLE weather (city text, year integer, warm_month text, cold_month text, average_high integer);")


def populate_cities_table(cur):
    cities = (
        ('New York City', 'NY'),
        ('Boston', 'MA'),
        ('Chicago', 'IL'),
        ('Miami', 'FL'),
        ('Dallas', 'TX'),
        ('Seattle', 'WA'),
        ('Portland', 'OR'),
        ('San Francisco', 'CA'),
        ('Los Angeles', 'CA'),    
    )
    cur.executemany("INSERT INTO cities VALUES(?,?)", cities)


def populate_weather_table(cur):
    weather = (
        ('New York City',   2013,    'July', 'January', 62), 
        ('Boston',          2013,    'July', 'January', 59), 
        ('Chicago',         2013,    'July', 'January', 59), 
        ('Miami',           2013,    'August', 'January', 84), 
        ('Dallas',          2013,    'July', 'January', 77), 
        ('Seattle',         2013,    'July', 'January', 61), 
        ('Portland',        2013,    'July', 'December', 63), 
        ('San Francisco',   2013,    'September', 'December', 64), 
        ('Los Angeles',     2013,    'September', 'December', 75), 
    )
    cur.executemany("INSERT INTO weather VALUES(?,?,?,?,?)", weather)


def join_data(cur):
    cmd = """
        SELECT name, state, year, warm_month, cold_month 
        FROM cities c
        INNER JOIN weather w 
            ON c.name = w.city;
    """
    cur.execute(cmd)
    return cur.fetchall()


def report_data(df):
    hottest_in_july = df[df["warm_month"] == 'July']
    formatted_july = ["{0}, {1}".format(row["name"], row["state"]) for _, row in hottest_in_july.iterrows()]
    print("The cities that are warmest in July are: {0}.".format("; ".join(formatted_july)))


def open_db():
    return lite.connect(DATABASE_NAME)


def main():
    with open_db() as con:
        cur = con.cursor()
        create_cities_table(cur)
        create_weather_table(cur)
        populate_cities_table(cur)
        populate_weather_table(cur)
        con.commit()
        data = join_data(cur)
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=cols)
        report_data(df)

if __name__ == '__main__':
    main()
