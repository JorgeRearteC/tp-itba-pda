# TP - ITBA - PYTHON DATA APPLICATIONS

## Flight Radar

### Objetivo del proyecto

El objetivo de este proyecto es desarrollar una solución automatizada de integración y procesamiento de datos utilizando Airflow, Docker, y la API de FlightRadar. 
La idea central es construir un pipeline ETL (Extracción, Transformación y Carga) eficiente, que permita recopilar y normalizar en tiempo real información relevante de vuelos operados por aerolíneas específicas. 
Esta solución será diseñada para ser fácilmente desplegable y escalable mediante contenedores Docker, asegurando portabilidad y simplicidad en su configuración.

### Tablas:
En el proyecto generamos dos tablas, flights y airlines.

- Flights: 
El objetivo de la presente es mantener las posiciones actuales de los aviones de Flybondi. 
En particular esta solución estaria orientada a un equipo que le de seguimiento a la parte operativa de los aviones propios.
*En un principio mi idea era generar un mapa interactivo para marcar las posiciones al estilo [Flight Radar](https://www.flightradar24.com/)*

- Airlines:
El objetivo es tener un listado de todas las aerolineas existentes del mundo, obteniendo de la misma tres datos, el nombre de la aerolinea, el codigo y el ICAO que es el código unico en el mundo definido por Org. de Aviación Civil Internacional. La misma establece normativas en común a nivel internacional. 


### Test:
En este punto, se han probado dos métodos del proceso etl_data_sdk_flight_radar.py que utilizan llamadas al SDK de Flight Radar. Ambas pruebas implementan mocking para simular el comportamiento de la API, lo que permite validar la lógica sin realizar llamadas reales a servicios externos. Esto nos da la confianza necesaria para asegurar que los resultados obtenidos sean los esperados.

- test_get_flybondi_flights: Esta prueba tiene como objetivo obtener el listado de vuelos disponibles. Se verifica que la función retorne correctamente los vuelos solicitados a través del SDK.
- test_get_flight_details: En esta prueba, buscamos obtener el detalle de los vuelos que fueron obtenidos en la prueba anterior. Se valida que la función devuelva la información detallada de cada vuelo correctamente.


### Instalación

1. Generar archivo .env en la carpeta raiz y completar los datos:
REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_USER=
REDSHIFT_PASS=
REDSHIFT_DB=
En caso de tener caracteres especiales utilizar comillas simples.

2. Generar imagen del proyecto:
Ejecutamos el siguiente comando: docker build -t tp-itba-pda .

3. Levantar Airflow:
Ejecutamos el siguiente comando: docker-compose up

4. De manera local sera posible visualizar los dags generado en: http://localhost:8080/home

Por defecto el usuario y clave son admin pero se podria generar un usuario propio con el siguiente comando:

airflow users create \
  --username admin_example \
  --firstname admin_example \
  --lastname admin_example \
  --email admin_example@admin_example.com \
  --role Admin \
  --password admin_example

### Links utiles

+ https://pypi.org/project/FlightRadarAPI/

### Trabajando con Docker

#### + Construir imagen: docker build -t tp-itba-pda .
#### + Ejecutar una imagen: docker-compose run --rm --entrypoint bash app-etl
#### + Consultar imagenes: docker images
#### + Borrar una imagen: docker rmi id
#### + Iniciar AirFlow Local: docker-compose up


