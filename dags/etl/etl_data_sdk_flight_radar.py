import os
import pandas as pd
from dotenv import load_dotenv
from FlightRadar24 import FlightRadar24API
from etl.modules.redshift_connection import RedshiftDatabase

load_dotenv()

def run_etl():
    """Función principal para ejecutar el proceso ETL."""
    fr_api = FlightRadar24API()

    airline_icao = "FBZ"  # FLYBONDI
    zone = fr_api.get_zones()["southamerica"]
    bounds = fr_api.get_bounds(zone)

    # Obtener los vuelos
    flybondi_flights = get_flybondi_flights(fr_api, airline_icao, bounds)
    flights_detail = get_flight_details(fr_api, flybondi_flights)
    df_flights_detail = pd.DataFrame(flights_detail)

    print('Cantidad de vuelos:', len(df_flights_detail))

    # Procesar los datos
    df_flights = process_flight_data(df_flights_detail)

    rd = redshift_connection()

    # Insertar en Redshift
    overwrite_into_redshift(df_flights,rd)

    df_flight_events_history = process_flight_events_data(df_flights)
    
    insert_into_redshift(df_flight_events_history,rd)


def get_flybondi_flights(api, airline_icao, bounds):
    """Buscamos todos los vuelos activos de la aerolinea deseada."""
    return api.get_flights(airline=airline_icao, bounds=bounds)


def get_flight_details(api, flights):
    """Obtenemos el detalle de los vuelos."""
    return [api.get_flight_details(flight) for flight in flights]

def process_flight_data(df_flights_detail):
    """Normaliza y procesa los datos de vuelos."""
    # Normalización de diferentes secciones del DataFrame
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

    return df_flights

def process_flight_events_data(df_flights):
        
    columns = [
        "id",
        "callsign",
        "number_default",
        "generic_status_text",
        "generic_status_type",
        "generic_eventtime_utc",
        "generic_eventtime_local",
        "countryid",
        "registration",
        "model_code",
        "code_iata",
        "origin_code_iata",
        "origin_position_latitude",
        "origin_position_longitude",
        "origin_position_altitude",
        "destination_code_iata",
        "destination_position_latitude",
        "destination_position_longitude",
        "destination_position_altitude",
        "scheduled_departure",
        "scheduled_arrival",
        "real_departure",
        "real_arrival",
        "estimated_departure",
        "estimated_arrival",
        "historical_flighttime",
        "historical_delay",
        "latitude",
        "longitude",
        "altitude",
        "speed",
        "capture_datetime_utc",
        "heading"
    ]
    df_flight_events_history = df_flights[columns]
    
    return df_flight_events_history

def redshift_connection():
    redshift_connection = RedshiftDatabase(
        username=os.getenv('REDSHIFT_USER'), 
        password=os.getenv('REDSHIFT_PASS'), 
        host=os.getenv('REDSHIFT_HOST'), 
        port=os.getenv('REDSHIFT_PORT'), 
        database=os.getenv('REDSHIFT_DB')
    )
    return redshift_connection


def insert_into_redshift(df_flight_events_history,redshift_connection):
    """Inserta el DataFrame en la base de datos Redshift."""

    df_flight_events_history = redshift_connection.order_columns(df_flight_events_history, 'flight_events_history', '2024_jorge_roberto_rearte_carvalho_schema')

    redshift_connection.insert(df_flight_events_history, 'flight_events_history', '2024_jorge_roberto_rearte_carvalho_schema')

def overwrite_into_redshift(df_flights,redshift_connection):
    """Inserta el DataFrame en la base de datos Redshift."""

    df_flights = redshift_connection.order_columns(df_flights, 'flights', '2024_jorge_roberto_rearte_carvalho_schema')

    redshift_connection.overwrite_table(df_flights, 'flights', '2024_jorge_roberto_rearte_carvalho_schema')