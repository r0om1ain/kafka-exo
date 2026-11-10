# Exercice 4 : transformation du flux weather_stream avec Spark et calcul des alertes
# Lancement (dans le conteneur pyspark_notebook) :
#   spark-submit work/ex4_transformation.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

spark = (
    SparkSession.builder.appName("ex4_transformation")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", "1")  # le worker a 2 coeurs : on en laisse un pour l'exercice 5
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schéma du JSON envoyé par le producteur de l'exercice 3 (on ne garde que ce qui sert)
schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("current", StructType([
        StructField("time", StringType()),
        StructField("temperature_2m", DoubleType()),
        StructField("wind_speed_10m", DoubleType()),
        StructField("weather_code", IntegerType()),
    ])),
])

# 1. Lecture du topic Kafka (value = le message en bytes, timestamp = heure d'écriture dans Kafka)
brut = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_stream")
    .option("startingOffsets", "earliest")
    .load()
)

# 2. On décode le JSON et on garde les colonnes demandées
meteo = (
    brut.select(
        col("timestamp").alias("event_time"),
        from_json(col("value").cast("string"), schema).alias("data"),
    )
    .select(
        "event_time",
        col("data.city").alias("city"),
        col("data.country").alias("country"),
        # l'API renvoie déjà des °C et des m/s (wind_speed_unit=ms) : pas de conversion à faire
        col("data.current.temperature_2m").alias("temperature"),
        col("data.current.wind_speed_10m").alias("windspeed"),
        col("data.current.weather_code").alias("weather_code"),
    )
    # le message {"msg": "Hello Kafka"} de l'exercice 1 n'a pas de ville : on l'enlève
    .filter(col("city").isNotNull() & col("temperature").isNotNull())
)

# 3. Niveaux d'alerte
meteo = meteo.withColumn(
    "wind_alert_level",
    when(col("windspeed") < 10, "level_0")
    .when(col("windspeed") <= 20, "level_1")
    .otherwise("level_2"),
)
meteo = meteo.withColumn(
    "heat_alert_level",
    when(col("temperature") < 25, "level_0")
    .when(col("temperature") <= 35, "level_1")
    .otherwise("level_2"),
)

# 4. Écriture dans weather_transformed : Kafka attend une colonne "value", on remet tout en JSON
sortie = meteo.select(to_json(struct("*")).alias("value"))

query = (
    sortie.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "weather_transformed")
    .option("checkpointLocation", "/home/jovyan/work/checkpoints/ex4")
    .outputMode("append")
    .start()
)

print("Transformation lancée : weather_stream -> weather_transformed")
query.awaitTermination()
