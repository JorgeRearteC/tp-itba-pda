import pytest
import pandas as pd
from unittest.mock import MagicMock
import etl_data_sdk_flight_radar as sdk_fr

def test_get_flybondi_flights():
    # Mock de la API y su respuesta
    api_mock = MagicMock()
    api_mock.get_flights.return_value = ["flight_1", "flight_2"]

    # Llamamos a la función
    flights = sdk_fr.get_flybondi_flights(api_mock, "FBZ", {"north": 10, "south": -10})

    # Verificamos los resultados
    assert flights == ["flight_1", "flight_2"]
    api_mock.get_flights.assert_called_once_with(airline="FBZ", bounds={"north": 10, "south": -10})

def test_get_flight_details():
    # Mock de la API y su respuesta
    api_mock = MagicMock()
    api_mock.get_flight_details.side_effect = [
        {"flight": "detail_1"}, 
        {"flight": "detail_2"}
    ]

    # Llamamos a la función
    flights = ["flight_1", "flight_2"]
    details = sdk_fr.get_flight_details(api_mock, flights)

    # Verificamos los resultados
    assert details == [{"flight": "detail_1"}, {"flight": "detail_2"}]
    api_mock.get_flight_details.assert_has_calls([MagicMock().call("flight_1"), MagicMock().call("flight_2")])

