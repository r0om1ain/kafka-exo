# TP Kafka : météo en temps réel

Kafka + Spark Structured Streaming + HDFS. Le sujet est dans `TP_KAFKA_1.pdf`.

## Démarrer l'environnement

```bash
docker compose up -d --build
```

Le fichier `docker-compose.override.yml` ne publie pas le port 9000 du namenode sur le Mac, car il était déjà pris par un autre service.
Dans les conteneurs, HDFS reste joignable sur `hdfs://namenode:9000`.

Les scripts sont dans `work/` et se lancent depuis le conteneur `pyspark_notebook`, soit avec un terminal JupyterLab (http://localhost:8888), soit avec `docker exec -it -w /home/jovyan pyspark_notebook <commande>`.

| Exercice | Fichier | Commande |
|---|---|---|
| 1 : topic + message statique | `work/ex1_producteur.py` | créer le topic (voir plus bas) puis `python work/ex1_producteur.py` |
| 2 : consommateur | `work/ex2_consommateur.py` | `python work/ex2_consommateur.py` |
| 3 : producteur météo | `work/ex3_current_weather.py` | `python work/ex3_current_weather.py` |
| 4 : transformation Spark + alertes | `work/ex4_transformation.py` | `spark-submit work/ex4_transformation.py` |
| 5 : fenêtre glissante | `work/ex5_agregats.py` | `spark-submit work/ex5_agregats.py` |
| 6 : stockage HDFS | `work/ex6_stockage_hdfs.py` | `python work/ex6_stockage_hdfs.py` |
| 7 : visualisation | `work/ex7_visualisation.ipynb` | à ouvrir dans JupyterLab |

Les exercices 3 à 6 tournent en continu : il faut un terminal par script, et ils doivent tourner en même temps.

## Création des topics (exercice 1)

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic weather_stream --bootstrap-server kafka:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic weather_transformed --bootstrap-server kafka:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092
```

## Vérifications utiles

```bash
# lire le topic transformé
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic weather_transformed --from-beginning

# voir les fichiers HDFS
docker exec namenode hdfs dfs -ls -R /hdfs-data
docker exec namenode hdfs dfs -cat "/hdfs-data/France/Paris/alerts.json"
```

## Choix faits

- Les coordonnées des villes ne sont pas données dans le sujet : j'ai pris celles des centres-villes.
- Seuils : vent `< 10` → level_0, `10 à 20` → level_1, `> 20` → level_2 (m/s) ; température `< 25` → level_0, `25 à 35` → level_1, `> 35` → level_2 (°C).
- L'API renvoie déjà des °C et des m/s (`wind_speed_unit=ms`), donc aucune conversion n'est nécessaire.
- Le message `{"msg": "Hello Kafka"}` de l'exercice 1 est filtré dans l'exercice 4 parce qu'il n'a pas de ville.
- `spark.cores.max = 1` dans chaque job Spark : le worker n'a que 2 cœurs, et sans cette limite le premier job les prendrait tous, ce qui bloquerait le second.
- Le notebook de l'exercice 7 utilise Spark en mode local (`local[*]`) : pendant que les exercices 4 et 5 tournent, le worker n'a plus de cœur libre, et le notebook resterait bloqué en attente.
- Les fichiers HDFS sont créés avec `replication=1` parce qu'il n'y a qu'un seul datanode : sinon, l'ajout en fin de fichier échoue.
- Open-Meteo ne met à jour les valeurs « current » que toutes les 15 minutes. On reçoit donc souvent la même valeur plusieurs fois de suite, et les courbes sont en escalier.
