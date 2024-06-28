import psycopg2
from psycopg2 import sql
import json
import urllib3
import boto3
from datetime import datetime, timedelta

secret_name = 'fishing-secrets'
insert_query = '''
    insert into fw1.t_forecast
    (request_date, forecast_date, location_name, temperature_min, temperature_max, cloud_cover_avg, precip_probability_avg, rain_intensity_avg, weather_code_min, weather_code_max)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''
request_time = 'NOW()'

http = urllib3.PoolManager()


def get_secret():
    secret_client = boto3.client('secretsmanager')
    secret_response = secret_client.get_secret_value(
        SecretId=secret_name)
    secret_dict = json.loads(secret_response['SecretString'])
    return secret_dict


def connect_to_db(secret_dict):
    conn = psycopg2.connect(
        host=secret_dict['host'],
        port=secret_dict['port'],
        dbname=secret_dict['db_name'],
        user=secret_dict['db_username'],
        password=secret_dict['db_password']
    )
    return conn


def get_weather(secret_dict):
    fields = {'apikey': secret_dict['tomorrow_io_apikey'],
              'location': secret_dict['rt_location'],
              'units': secret_dict['rt_units']}
    response = http.request('GET',
                            'https://api.tomorrow.io/v4/weather/forecast',
                            fields=fields)
    data = json.loads(response.data)
    return data


def insert_into_db(conn, query, data):
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()
    cur.close()


def lambda_handler(event, context):
    secret_dict = get_secret()
    weather_data = get_weather(secret_dict)
    db_connection = connect_to_db(secret_dict)
    location_name = weather_data['location']['name']
    request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    current_date = datetime.now()

    for i, day_data in enumerate(weather_data["timelines"]["daily"], start=0):
        values = day_data["values"]
        forecast_date = day_data["time"]
        data_to_insert = (
            request_time,
            forecast_date,
            location_name,
            values.get("temperatureMin"),
            values.get("temperatureMax"),
            values.get("cloudCoverAvg"),
            values.get("precipitationProbabilityAvg"),
            values.get("rainIntensityAvg"),
            values.get("weatherCodeMin"),
            values.get("weatherCodeMax")
        )
        insert_into_db(db_connection, insert_query, data_to_insert)

    db_connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Weather data successfully inserted!')
    }

