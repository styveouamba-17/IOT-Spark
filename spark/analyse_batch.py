"""
Couche batch : analyse historique des mesures archivées en Parquet par le job de streaming.

Le streaming répond vite mais ne voit que les dernières secondes. Ici, on relit TOUT l'historique :
les mesures très en retard écartées des fenêtres sont prises en compte, et les doublons sont retirés.

Lancement (le job de streaming peut continuer à tourner) :
  docker compose exec spark /opt/spark/bin/spark-submit /app/analyse_batch.py
  ou en local : CHEMIN_DONNEES=./donnees spark-submit spark/analyse_batch.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CHEMIN_DONNEES = os.getenv("CHEMIN_DONNEES", "./donnees")
TEMP_MIN, TEMP_MAX = 2.0, 8.0


def main():
    spark = (SparkSession.builder.appName("iot-analyse-batch")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.port", "4041")          # 4040 est déjà pris par le job de streaming
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    brut = spark.read.parquet(f"{CHEMIN_DONNEES}/archive/mesures")
    mesures = brut.dropDuplicates(["message_id"])        # une mesure ne compte qu'une fois
    hors_plage = (F.col("temperature") > TEMP_MAX) | (F.col("temperature") < TEMP_MIN)

    par_zone = (mesures.groupBy("zone")
                .agg(F.count(F.lit(1)).alias("nb_mesures"),
                     F.countDistinct("sensor_id").alias("nb_capteurs"),
                     F.round(F.avg("temperature"), 2).alias("temp_moy"),
                     F.round(F.min("temperature"), 2).alias("temp_min"),
                     F.round(F.max("temperature"), 2).alias("temp_max"),
                     F.round(100 * F.avg(F.when(hors_plage, 1).otherwise(0)), 2).alias("pct_hors_plage"),
                     F.min("event_time").alias("premiere_mesure"),
                     F.max("event_time").alias("derniere_mesure")))

    # minutes pendant lesquelles la température MOYENNE de la chambre est sortie de la plage
    minutes = (mesures.groupBy("zone", F.window("event_time", "1 minute"))
               .agg(F.avg("temperature").alias("t"))
               .where((F.col("t") > TEMP_MAX) | (F.col("t") < TEMP_MIN))
               .groupBy("zone").agg(F.count(F.lit(1)).alias("minutes_hors_plage")))

    rapport = (par_zone.join(minutes, "zone", "left")
               .fillna(0, subset=["minutes_hors_plage"])
               .orderBy("zone"))

    print(f"\nMesures archivées : {brut.count()} (dont {brut.count() - mesures.count()} doublons retirés)\n")
    rapport.show(truncate=False)

    par_capteur = (mesures.where(hors_plage).groupBy("zone", "sensor_id")
                   .agg(F.count(F.lit(1)).alias("mesures_hors_plage"),
                        F.round(F.max("temperature"), 2).alias("temp_max"))
                   .orderBy(F.desc("mesures_hors_plage")))
    print("Capteurs ayant relevé des températures hors plage :")
    par_capteur.show(truncate=False)

    sortie = f"{CHEMIN_DONNEES}/rapports/synthese_par_zone"
    rapport.coalesce(1).write.mode("overwrite").option("header", True).csv(sortie)
    print(f"Rapport CSV écrit dans {sortie}")
    spark.stop()


if __name__ == "__main__":
    main()
