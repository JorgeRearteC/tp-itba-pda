import os
import pandas as pd
from dotenv import load_dotenv
from FlightRadar24 import FlightRadar24API
from etl.modules.redshift_connection import RedshiftDatabase

load_dotenv()

def run_etl():
    """Función principal para ejecutar el proceso ETL."""
    fr_api = FlightRadar24API()

    # Obtener los vuelos
    airlines = get_airlines(fr_api)
    
    print('Cantidad de aerolineas:', len(airlines))

    # Procesar los datos
    df_airlines = process_airlines_data(airlines)

    # Insertar en Redshift
    insert_into_redshift(df_airlines)


def get_airlines(api):
    """Buscamos todos los vuelos activos de la aerolinea deseada."""
    return api.get_airlines()


def process_airlines_data(airlines):
    df_airlines = pd.DataFrame(airlines)
    df_airlines.columns = df_airlines.columns.str.lower()
    df_airlines[['name', 'code', 'icao']] = df_airlines[['name', 'code', 'icao']].apply(lambda x: x.str.upper())
    df_airlines.fillna('S/D', inplace=True)
    df_airlines.replace('', 'S/D', inplace=True)
    return df_airlines


def insert_into_redshift(df_airlines):
    """Inserta el DataFrame en la base de datos Redshift."""
    rd = RedshiftDatabase(
        username=os.getenv('REDSHIFT_USER'), 
        password=os.getenv('REDSHIFT_PASS'), 
        host=os.getenv('REDSHIFT_HOST'), 
        port=os.getenv('REDSHIFT_PORT'), 
        database=os.getenv('REDSHIFT_DB')
    )

    # Insertar datos en Redshift
    rd.insert(df_airlines, 'airlines', '2024_jorge_roberto_rearte_carvalho_schema')
