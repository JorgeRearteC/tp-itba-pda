# TP - ITBA - PYTHON DATA APPLICATIONS

## Flight Radar

### Objetivo del proyecto

El objetivo de este proyecto es desarrollar una solución automatizada de integración y procesamiento de datos utilizando Airflow, Docker, y la API de FlightRadar. 
La idea central es construir un pipeline ETL (Extracción, Transformación y Carga) eficiente, que permita recopilar y normalizar en tiempo real información relevante de vuelos operados por aerolíneas específicas. 
Esta solución será diseñada para ser fácilmente desplegable y escalable mediante contenedores Docker, asegurando portabilidad y simplicidad en su configuración.


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


