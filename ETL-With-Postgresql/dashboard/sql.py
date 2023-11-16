import streamlit as st
import psycopg2, configparser
import pandas as pd


config = configparser.ConfigParser(); config.read('config.ini')
conn = psycopg2.connect(
    dbname = config.get("DATABASE", "dbname"),
    user = config.get("DATABASE", "ConnectionUser"),
    password = config.get("DATABASE", "ConnectionPassword"),
    host = config.get("DATABASE", "host"),
    port = config.get("DATABASE", "port")
)

def get_all_bike_types():
    cursor=conn.cursor()
    cursor.execute("SELECT DISTINCT rideable_type from warehouse.test")
    bike_types = []
    for item in cursor.fetchall():
        bike_types.append(item[0])
    cursor.close()
    return bike_types

def count_bikes_with_names(rideable_type):
    cursor=conn.cursor()
    cursor.execute(f"SELECT COUNT(*) from warehouse.test where rideable_type='{rideable_type}'")
    count = cursor.fetchall()[0][0]
    cursor.close()
    return count

def plot_map_starting(rideable_type):
    cursor=conn.cursor()
    cursor.execute(f"SELECT start_lat, start_lng from warehouse.test where rideable_type='{rideable_type}'")
    df = pd.DataFrame(cursor.fetchall(), columns=['lat', 'lon']).dropna()
    cursor.close()
    return df

def count_member_types(rideable_type):
    cursor=conn.cursor()
    cursor.execute(f"""SELECT member_casual, COUNT(*) AS count
                FROM warehouse.test where rideable_type='{rideable_type}'
                GROUP BY member_casual
                ORDER BY count DESC""")
    data = cursor.fetchall()
    member_count = None
    casual_count = None

    for row in data:
        if row[0] == 'member':
            member_count = row[1]
        elif row[0] == 'casual':
            casual_count = row[1]
    cursor.close()
    return member_count, casual_count

def plot_map_ending(rideable_type):
    cursor=conn.cursor()
    cursor.execute(f"SELECT end_lat, end_lng from warehouse.test where rideable_type='{rideable_type}'")
    df = pd.DataFrame(cursor.fetchall(), columns=['lat', 'lon']).dropna()
    cursor.close()
    return df

def fav_start_station(rideable_type):
    cursor=conn.cursor()
    cursor.execute(f"""SELECT start_station_name, COUNT(*) AS count
                    FROM warehouse.test where rideable_type='{rideable_type}'
                    GROUP BY start_station_name
                    ORDER BY count DESC""")
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['station_name', 'count']).head(2).dropna()
    cursor.close()
    return str(df.loc[0].station_name), str(df.loc[1].station_name)
