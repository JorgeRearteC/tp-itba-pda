# Usamos la imagen base de Python 3.8 con Buster
FROM python:3.8-buster

# Definimos variables de entorno para Airflow
ENV AIRFLOW_HOME=/opt/airflow \
    AIRFLOW_VERSION=2.7.2 \
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt"

# Copiamos el archivo de requerimientos al contenedor
COPY requirements.txt .

# Instalamos dependencias del sistema necesarias para PostgreSQL
RUN apt-get update && apt-get install -y \
    libpq-dev gcc curl \
    && rm -rf /var/lib/apt/lists/*

# Instalamos Airflow y dependencias adicionales desde el archivo de restricciones
RUN pip install --no-cache-dir apache-airflow[postgres]==${AIRFLOW_VERSION} \
    --constraint "${CONSTRAINT_URL}" \
    && pip install --no-cache-dir -r requirements.txt

# Creamos directorios para Airflow
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/dags/etl $AIRFLOW_HOME/dags/etl/modules $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins

# Exponemos el puerto 8080 para la interfaz web de Airflow
EXPOSE 8080

# Copiamos los DAGs, ETLs y Modulos
COPY ./dags $AIRFLOW_HOME/dags
RUN ls -lR $AIRFLOW_HOME/dags/ && echo "Archivos copiados correctamente"


# COPY ./src/etl $AIRFLOW_HOME/scripts
# COPY ./src/etl/modules $AIRFLOW_HOME/scripts/modules

# Comando por defecto: usar LocalExecutor con PostgreSQL
CMD ["airflow", "webserver"]




