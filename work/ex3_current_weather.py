# Exercice 3 : producteur current_weather
# Toutes les 30 secondes, on interroge Open-Meteo pour chaque ville
# et on envoie la réponse (avec city et country en plus) dans weather_stream.
import json
import time

import requests
from kafka import KafkaProducer

TOPIC = "weather_stream"
URL = (
    "https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}"
    "&current=temperature_2m,wind_speed_10m,weather_code&wind_speed_unit=ms"
)

VILLES = [
    {"city": "Paris", "country": "France", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Seville", "country": "Spain", "latitude": 37.3891, "longitude": -5.9845},
    {"city": "Dubai", "country": "United Arab Emirates", "latitude": 25.2048, "longitude": 55.2708},
    {"city": "Reykjavik", "country": "Iceland", "latitude": 64.1466, "longitude": -21.9426},
    {"city": "Wellington", "country": "New Zealand", "latitude": -41.2865, "longitude": 174.7762},
]

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    for ville in VILLES:
        try:
            reponse = requests.get(URL.format(latitude=ville["latitude"], longitude=ville["longitude"]), timeout=10)
            reponse.raise_for_status()
            data = reponse.json()
            data["city"] = ville["city"]
            data["country"] = ville["country"]
            producer.send(TOPIC, data)
            print(ville["city"], "->", data["current"])
        except Exception as e:
            # si l'API ne répond pas pour une ville, on passe à la suivante
            print("Erreur pour", ville["city"], ":", e)

    producer.flush()
    time.sleep(30)
