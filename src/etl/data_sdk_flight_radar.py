import pandas as pd

from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()

airline_icao = "FBZ" # FLYBONDI
aircraft_type = "B738" # TIPO DE AVIONES DE FLYBONDI

zone = fr_api.get_zones()["southamerica"]
bounds = fr_api.get_bounds(zone)

flybondi_flights = fr_api.get_flights(
    aircraft_type = aircraft_type,
    airline = airline_icao,
    bounds = bounds
)

flights_detail = []

for flight in flybondi_flights:
    print(flight)
    flight_detail = fr_api.get_flight_details(flight)
    print(flight_detail)
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

df_flights = df_normalize_identification.join(df_normalize_status).join(df_normalize_aircraft).join(df_normalize_airline).join(df_normalize_airport).join(df_normalize_time)



### RUTA DEL VUELO
df_explode_trail = df_flights_detail.explode('trail')
df_explode_trail = pd.DataFrame(df_explode_trail[['identification','trail']])
df_normalize_trail = pd.json_normalize(df_explode_trail['trail'])
df_explode_trail = df_explode_trail.join(df_normalize_trail)
df_normalize_id = pd.json_normalize(df_explode_trail['identification'])
df_explode_trail = df_explode_trail.join(df_normalize_id)


