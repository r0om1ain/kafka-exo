# Exercice 2 : consommateur qui affiche les messages en temps réel
import json

from kafka import KafkaConsumer

TOPIC = "weather_stream"  # topic à lire (changer ici pour lire weather_transformed par exemple)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",  # si c'est la première lecture, on part du début du topic
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("En attente de messages sur", TOPIC, "... (Ctrl+C pour arrêter)")
for message in consumer:
    # la boucle ne s'arrête jamais : elle attend les nouveaux messages
    print(f"[partition {message.partition} | offset {message.offset}] {message.value}")
