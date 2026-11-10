# Exercice 1 : producteur simple
# Le topic weather_stream est créé avant avec la commande (voir README) :
#   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic weather_stream --bootstrap-server kafka:9092
import json

from kafka import KafkaProducer

TOPIC = "weather_stream"

# value_serializer : transforme le dictionnaire Python en JSON (bytes) avant l'envoi
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

message = {"msg": "Hello Kafka"}
producer.send(TOPIC, message)
producer.flush()  # on attend que le message soit vraiment parti avant de quitter

print("Message envoyé dans", TOPIC, ":", message)
