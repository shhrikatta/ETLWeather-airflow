from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import json
import requests

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
API_CONN_ID = 'open_meteo_api'
POSTGRESS_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
        dag_id='weather-etl-pipeline',
        default_args=default_args,
        schedule_interval="@daily",
        catchup=False
) as dags:
    @task
    def extract_weather_data():
        """ extract the weather data from open metro api using airflow connection """
        # use http hook to fetch data
        http_hook = HttpHook(
            http_conn_id=API_CONN_ID,
            method='GET'
        )

        # https://api.open-meteo.com
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # make the request via http hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Failed to fetch weather data. Status code: {response.status_code}')


    @task
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_current_weather = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_current_weather


    @task
    def load_weather_data(transformed_current_weather):
        """ load the weather data into postgres database using airflow connection """
        # use postgres hook to connect to postgres database
        pg_hook = PostgresHook(postgres_conn_id=POSTGRESS_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cursor.execute(
            """
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                transformed_current_weather['latitude'],
                transformed_current_weather['longitude'],
                transformed_current_weather['temperature'],
                transformed_current_weather['windspeed'],
                transformed_current_weather['winddirection'],
                transformed_current_weather['weathercode']
            )
        )

        conn.commit()
        cursor.close()


    weather_data = extract_weather_data()
    transformed_current_weather = transform_weather_data(weather_data)
    load_weather_data(transformed_current_weather)
