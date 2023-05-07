import time
import hmac
import hashlib
import requests
import json
import pandas as pd
import datetime
import pytz
import json
import os
from sqlalchemy import create_engine

# Global Variable declaration - API key, API secret, base URL, RDS username, RDS password, RDS host, RDS databasename, RDS Host name


api_key = os.environ['API_KEY']
api_secret = os.environ['API_SECRET']
time_stamp = str(int(time.time()))
base_url = "https://api.weatherlink.com/v2"
username = os.environ['USERNAME']
password = os.environ['PASSWORD']
port = 5432
host = os.environ['HOST']
db_name = os.environ['DB_NAME']

# Function to calculate api signature using hmac-SHA256


def calculate_signature(api_secret, message_to_hash):
    signature = hmac.new(api_secret.encode("UTF-8"),
                         message_to_hash.encode("UTF-8"), hashlib.sha256).hexdigest()
    return (signature)

# Function to get the station_id


def get_stationid(api_key, api_secret, time_stamp, base_url):
    string_to_hash = "api-key"+api_key+"t"+time_stamp
    api_signature = calculate_signature(api_secret, string_to_hash)
    stid_url = base_url + "/stations?" + "api-key=" + api_key + "&t=" + \
        time_stamp + "&api-signature=" + api_signature
    station = json.loads(requests.get(stid_url).text)
    for st_id in station["stations"]:
        station_id = (str((st_id["station_id"])))
    return (station_id)

# Function to get the live data


def get_livedata(api_key, api_secret, time_stamp, station_id, base_url):
    string_to_hash = "api-key"+api_key+"station-id"+station_id+"t"+time_stamp
    api_signature = calculate_signature(api_secret, string_to_hash)
    live_data_url = base_url + "/current/"+station_id+"?api-key=" + \
        api_key+"&t="+time_stamp+"&api-signature="+api_signature
    live = json.loads(requests.get(live_data_url).text)
    return (live)

# Function to convert epoch time stamp to readable format


def timeconversion(epoch_timestamp):
    if epoch_timestamp == 0:
        return ("0")
    else:
        epoch_timestamp = int(epoch_timestamp)
        utc_dt = datetime.datetime.fromtimestamp(epoch_timestamp, pytz.utc)
        utc_dt = utc_dt.replace(second=0, microsecond=0)
        brisbane = pytz.timezone("Australia/Brisbane")
        local_dt = brisbane.normalize(utc_dt.astimezone(brisbane))
        time = local_dt.strftime("%Y-%m-%d %H:%M:%S")
        return (time)

# Function to update the data to postgresSql


def to_postgresdb(username, password, host, db_name, sensor_type, df):
    engine = create_engine(
        f'postgresql://{username}:{password}@{host}/{db_name}')
    df.to_sql(f'sensor_type_{sensor_type}_15min',
              engine, if_exists="append")

# Perform the API call and save the collected data to postgreSql.


def lambda_handler(event, context):

    station_id = get_stationid(api_key, api_secret, time_stamp, base_url)
    live_data = get_livedata(
        api_key, api_secret, time_stamp, station_id, base_url)

    for sensor in live_data['sensors']:
        sensor_type = sensor['sensor_type']
        data = sensor['data']
        df = pd.DataFrame(data)
        df.fillna(0, inplace=True)
        converted_time_stamp = timeconversion(df['ts'].iloc[0])
        df['ts'] = converted_time_stamp

        # convert inches of mercury to hpa
        if (sensor_type == 242):
            df.iloc[0:2, 0:2] = df.iloc[0:2, 0:2] * 33.86389
            df.iloc[:, 0:2] = df.iloc[:, 0:2].round(2)

        # convert degree fahrenheit to degree celsius
        if (sensor_type == 243):
            df.iloc[0:3, 0:3] = (df.iloc[0:3, 0:3]-32)*(5/9)
            df.iloc[:, 0:3] = df.iloc[:, 0:3].round(2)

        if (sensor_type == 43):
            # convert miles/hr to km/hr
            list_wind_speed = ['wind_speed_last', 'wind_speed_hi_last_2_min',   'wind_speed_hi_last_10_min',
                               'wind_speed_avg_last_2_min', 'wind_speed_avg_last_10_min',   'wind_speed_avg_last_1_min']
            for i in list_wind_speed:
                df[i] = (df[i]*1.609).round(2)
            # convert degree fahrenheit to degree celsius
            list_temp = ['wind_chill', 'thw_index', 'wet_bulb',
                         'dew_point', 'heat_index', 'temp', 'thsw_index']
            for i in list_temp:
                df[i] = ((df[i]-32)*(5/9)).round(2)

            list_start_end_time = ['rain_storm_last_start_at',
                                   'rain_storm_last_end_at', 'rain_storm_start_time']
            for i in list_start_end_time:
                timestamp_epoch_val = df[i].astype(int)
                df[i] = df[i].apply(lambda x: 0 if x ==
                                    0 else timeconversion(x))

        to_postgresdb(username, password, host, db_name, sensor_type, df)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Data collected')
    }
