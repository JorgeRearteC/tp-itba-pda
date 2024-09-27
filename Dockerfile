FROM python:3.8-buster

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080 

CMD ["airflow", "standalone"]


