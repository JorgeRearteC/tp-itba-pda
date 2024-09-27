import os
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()

from modules.redshift_connection import RedshiftDatabase

rd = RedshiftDatabase(os.getenv('REDSHIFT_USER'),os.getenv('REDSHIFT_PASS'),os.getenv('REDSHIFT_HOST'),os.getenv('REDSHIFT_PORT'),os.getenv('REDSHIFT_DB'))

airline_icao = "FBZ" # FLYBONDI
# aircraft_type = "B738" # TIPO DE AVIONES DE FLYBONDI

zone = fr_api.get_zones()["southamerica"]
bounds = fr_api.get_bounds(zone)

flybondi_flights = fr_api.get_flights(
    # aircraft_type = aircraft_type,
    airline = airline_icao,
    bounds = bounds
)

flights_detail = []

for flight in flybondi_flights:
    flight_detail = fr_api.get_flight_details(flight)
    flights_detail.append(flight_detail)


df_flights_detail = pd.DataFrame(flights_detail)

print('Cantidad de vuelos:',len(df_flights_detail))

### INFO GENERAL DEL VUELO
df_normalize_identification = pd.json_normalize(df_flights_detail['identification'])
df_normalize_status = pd.json_normalize(df_flights_detail['status'])
df_normalize_aircraft = pd.json_normalize(df_flights_detail['aircraft'])
df_normalize_airline = pd.json_normalize(df_flights_detail['airline'])
df_normalize_airport = pd.json_normalize(df_flights_detail['airport'])
df_normalize_time = pd.json_normalize(df_flights_detail['time'])

### UBICACION
df_explode_trail = df_flights_detail.explode('trail')
df_explode_trail = pd.DataFrame(df_explode_trail[['identification','trail']])
df_explode_trail

df_normalize_trail = pd.json_normalize(df_explode_trail['trail'])
df_normalize_trail_identification = pd.json_normalize(df_explode_trail['identification'])
df_trail = pd.concat([df_normalize_trail,df_normalize_trail_identification], axis=1)

df_trail = df_trail.sort_values(by=['id', 'ts'])
df_trail = df_trail.drop_duplicates(subset=['id'], keep='last')
df_trail = df_trail.drop(columns=['row','callsign','number.default','number.alternative'])

df_flights = df_normalize_identification.join(df_normalize_status).join(df_normalize_aircraft).join(df_normalize_airline).join(df_normalize_airport).join(df_normalize_time).merge(df_trail,on=['id'],how='left')

df_flights.columns = df_flights.columns.str.replace('.','_').str.lower()
df_flights = df_flights.rename(columns={'lat':'latitude','lng':'longitude','alt':'altitude','spd':'speed','ts':'capture_datetime_utc','hd':'heading'})

timestamp_fields = [
    'generic_eventtime_utc',
    'generic_eventtime_local',
    'scheduled_departure',
    'scheduled_arrival',
    'real_departure',
    'real_arrival',
    'estimated_departure',
    'estimated_arrival',
    'other_eta',
    'other_updated',
    'capture_datetime_utc'
]

for field in timestamp_fields:
    df_flights[field] = pd.to_datetime(df_flights[field], unit='s')
    

df_flights = rd.order_columns(df_flights,'flights','2024_jorge_roberto_rearte_carvalho_schema')
    
rd.insert(df_flights,'flights','2024_jorge_roberto_rearte_carvalho_schema')
