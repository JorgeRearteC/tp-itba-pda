import pandas as pd
import time
from sqlalchemy import create_engine,text
from sqlalchemy.exc import OperationalError

class RedshiftDatabase:
    
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = self.create_engine()
                
    def create_engine(self):
        """Crea el engine de la base de datos."""
        return create_engine(f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}')
    
    def recreate_engine(self):
        """Recrea el engine de la base de datos."""
        self.engine = self.create_engine()

    def select(self, query):
        """Ejecuta una consulta SELECT y devuelve un DataFrame."""
        print('Start select.')
        while True:
            try:
                df = pd.read_sql(text(query), self.engine)
                print("Conexión exitosa.")
                return df
            except OperationalError:
                print("Error de conexión. Reintentando...")
                time.sleep(1)  # Espera antes de reintentar
                self.recreate_engine()

    def overwrite_table(self, df, table_name, schema):
        """Inserta un DataFrame en una tabla de Redshift."""
        print('Start insert.')
        while True:
            try:
                df.to_sql(table_name, self.engine, schema=schema, index=False, if_exists='replace', method='multi')
                print("Inserción exitosa.")
                break
            except OperationalError:
                print("Error de conexión. Reintentando...")
                time.sleep(1)  # Espera antes de reintentar
                self.recreate_engine()
    
    def insert(self, df, table_name, schema):
        """Inserta un DataFrame en una tabla de Redshift."""
        print('Start insert.')
        while True:
            try:
                df.to_sql(table_name, self.engine, schema=schema, index=False, if_exists='append', method='multi')
                print("Inserción exitosa.")
                break
            except OperationalError:
                print("Error de conexión. Reintentando...")
                time.sleep(1)  # Espera antes de reintentar
                self.recreate_engine()

    def order_columns(self, df, table_name, schema):
        """Ordena las columnas de un DataFrame según la tabla en Redshift."""
        print('Start order columns.')
        query = f"""
            SELECT 
                column_name
            FROM information_schema.columns
            WHERE 
                table_schema = '{schema}'
                AND 
                table_name = '{table_name}'
            ORDER BY 
                ordinal_position ASC
        """
        df_columns = self.select(query)
        columns_in_table = df_columns['column_name'].tolist()

        return df[columns_in_table]
