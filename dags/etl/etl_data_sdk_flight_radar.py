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

    # Insertar en Redshift
    insert_into_redshift(df_flights)


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


def insert_into_redshift(df_flights):
    """Inserta el DataFrame en la base de datos Redshift."""
    rd = RedshiftDatabase(
        username=os.getenv('REDSHIFT_USER'), 
        password=os.getenv('REDSHIFT_PASS'), 
        host=os.getenv('REDSHIFT_HOST'), 
        port=os.getenv('REDSHIFT_PORT'), 
        database=os.getenv('REDSHIFT_DB')
    )

    # Reordenar columnas según Redshift
    df_flights = rd.order_columns(df_flights, 'flights', '2024_jorge_roberto_rearte_carvalho_schema')

    # Insertar datos en Redshift
    rd.insert(df_flights, 'flights', '2024_jorge_roberto_rearte_carvalho_schema')
