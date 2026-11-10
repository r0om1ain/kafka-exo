# Exercice 5 : agrégats en temps réel sur une fenêtre glissante
# Lancement (dans le conteneur pyspark_notebook, pendant que l'exercice 4 tourne) :
#   spark-submit work/ex5_agregats.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, max, min, sum, when, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

spark = (
    SparkSession.builder.appName("ex5_agregats")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", "1")
    .config("spark.sql.shuffle.partitions", "4")  # 200 par défaut : beaucoup trop pour si peu de données
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schéma des messages écrits par l'exercice 4
schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("weather_code", IntegerType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
])

flux = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_transformed")
    .option("startingOffsets", "earliest")
    .load()
)

meteo = flux.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


# petite fonction pour compter les lignes qui vérifient une condition
def compter(condition):
    return sum(when(condition, 1).otherwise(0))


agregats = (
    meteo
    # watermark : on accepte des messages jusqu'à 1 minute en retard, après la fenêtre est fermée
    .withWatermark("event_time", "1 minute")
    # fenêtre de 1 minute qui avance toutes les 30 secondes
    .groupBy(window("event_time", "1 minute", "30 seconds"), "country", "city")
    .agg(
        compter(col("wind_alert_level") == "level_1").alias("vent_level_1"),
        compter(col("wind_alert_level") == "level_2").alias("vent_level_2"),
        compter(col("heat_alert_level") == "level_1").alias("chaleur_level_1"),
        compter(col("heat_alert_level") == "level_2").alias("chaleur_level_2"),
        avg("temperature").alias("temp_moyenne"),
        min("temperature").alias("temp_min"),
        max("temperature").alias("temp_max"),
    )
    .withColumn(
        "total_alertes",
        col("vent_level_1") + col("vent_level_2") + col("chaleur_level_1") + col("chaleur_level_2"),
    )
    .select(
        col("window.start").alias("debut"),
        col("window.end").alias("fin"),
        "country", "city",
        "vent_level_1", "vent_level_2", "chaleur_level_1", "chaleur_level_2", "total_alertes",
        "temp_moyenne", "temp_min", "temp_max",
    )
)

query = (
    agregats.writeStream.format("console")
    .outputMode("update")  # on n'affiche que les fenêtres qui ont changé depuis le dernier batch
    .option("truncate", "false")
    .option("numRows", 50)
    .option("checkpointLocation", "/home/jovyan/work/checkpoints/ex5")
    .start()
)

query.awaitTermination()
