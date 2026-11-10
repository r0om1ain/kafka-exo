# Exercice 6 : on lit weather_transformed et on range chaque message dans HDFS
# Fichier : /hdfs-data/{country}/{city}/alerts.json (une ligne JSON par message, ajoutée à la fin)
import json

from hdfs import InsecureClient
from kafka import KafkaConsumer

TOPIC = "weather_transformed"

# WebHDFS : API HTTP du namenode (port 9870)
client = InsecureClient("http://namenode:9870", user="root")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:9092",
    group_id="ex6-hdfs",           # Kafka retient où on s'est arrêté : pas de doublons si on relance
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Écriture des messages de", TOPIC, "dans HDFS ... (Ctrl+C pour arrêter)")
for message in consumer:
    data = message.value
    chemin = f"/hdfs-data/{data['country']}/{data['city']}/alerts.json"
    ligne = json.dumps(data) + "\n"

    if client.status(chemin, strict=False) is None:
        # premier message de la ville : on crée le fichier (les dossiers sont créés automatiquement)
        # replication=1 car il n'y a qu'un seul datanode, sinon l'ajout en fin de fichier échoue
        client.write(chemin, data=ligne, encoding="utf-8", replication=1)
    else:
        # ensuite on ajoute toujours à la fin : le fichier n'est jamais réécrit
        client.write(chemin, data=ligne, encoding="utf-8", append=True)

    print(chemin, "<-", data["event_time"], data["temperature"], "°C", data["windspeed"], "m/s")
