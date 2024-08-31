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
    
    
# df_flights_datail = pd.DataFrame(flights_detail)
