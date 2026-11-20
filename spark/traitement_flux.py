"""
Traitement du flux IoT avec Spark Structured Streaming (micro-lots).

Le job lit les mesures des capteurs dans le topic Kafka `iot-capteurs` et lance 4 requêtes continues :

  requête   traitement                                             sortie            état
  -------   ----------------------------------------------------   ---------------   ---------
  rejets    messages illisibles ou valeurs impossibles              topic iot-rejets  sans état
  alertes   mesures hors plage (2–8 °C, humidité > 85 %)            topic iot-alertes sans état
  stats     agrégats par chambre sur fenêtres glissantes 30 s / 10 s topic iot-stats   avec état
            (temps de l'événement, watermark 30 s, sans doublons)
  archive   mesures valides, pour l'analyse historique (batch)      fichiers Parquet  sans état

Lancement (voir README) :
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 traitement_flux.py
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --- Paramètres (modifiables par variables d'environnement) -----------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_CAPTEURS = os.getenv("TOPIC_CAPTEURS", "iot-capteurs")
TOPIC_ALERTES = os.getenv("TOPIC_ALERTES", "iot-alertes")
TOPIC_STATS = os.getenv("TOPIC_STATS", "iot-stats")
TOPIC_REJETS = os.getenv("TOPIC_REJETS", "iot-rejets")
CHEMIN_DONNEES = os.getenv("CHEMIN_DONNEES", "./donnees")      # archive Parquet + checkpoints

TEMP_MIN, TEMP_MAX = 2.0, 8.0          # plage de consigne des chambres froides (°C)
HUMIDITE_MAX = 85.0                    # % d'humidité relative
MARGE_CRITIQUE = 2.0                   # au-delà de 8 + 2 °C (ou en dessous de 2 − 2 °C) : alerte critique
PLAGE_PHYSIQUE_TEMP = (-40.0, 60.0)    # en dehors : le capteur est défectueux, la mesure est rejetée

FENETRE = os.getenv("FENETRE", "30 seconds")        # longueur x de la fenêtre glissante
GLISSEMENT = os.getenv("GLISSEMENT", "10 seconds")  # période de glissement y (y < x : fenêtres chevauchantes)
WATERMARK = os.getenv("WATERMARK", "30 seconds")    # retard maximal accepté (temps de l'événement)

SCHEMA_MESURE = T.StructType([
    T.StructField("message_id", T.StringType()),
    T.StructField("sensor_id", T.StringType()),
    T.StructField("zone", T.StringType()),
    T.StructField("temperature", T.DoubleType()),
    T.StructField("humidite", T.DoubleType()),
    T.StructField("event_time", T.TimestampType()),    # temps de l'événement (fixé par le capteur)
])


# --- Session Spark --------------------------------------------------------------------------
def creer_session(nom: str = "iot-chaine-du-froid") -> SparkSession:
    spark = (SparkSession.builder
             .appName(nom)
             .config("spark.sql.session.timeZone", "UTC")
             # 200 partitions par défaut : bien trop pour 12 capteurs, et chaque partition porte un état
             .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "4"))
             .config("spark.sql.streaming.metricsEnabled", "true")
             .config("spark.ui.showConsoleProgress", "false")       # journaux lisibles
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --- Source -----------------------------------------------------------------------------------
def lire_kafka(spark: SparkSession) -> DataFrame:
    """Flux brut : une ligne par message Kafka, la valeur JSON en texte dans la colonne `value`."""
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC_CAPTEURS)
            .option("startingOffsets", "latest")      # au tout premier lancement ; ensuite : le checkpoint
            .option("maxOffsetsPerTrigger", 5000)     # contre-pression : au plus 5 000 messages par micro-lot
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("value").cast("string").alias("value")))


# --- Transformations (indépendantes de la source et des sorties) -------------------------------
def parser(brut: DataFrame) -> DataFrame:
    """JSON -> colonnes typées. Un message illisible donne des colonnes nulles (mode PERMISSIVE)."""
    return (brut
            .withColumn("m", F.from_json("value", SCHEMA_MESURE))
            .select(F.col("value").alias("brut"), "m.*"))


def condition_validite():
    t_min, t_max = PLAGE_PHYSIQUE_TEMP
    cond = (F.col("message_id").isNotNull() & F.col("sensor_id").isNotNull()
            & F.col("zone").isNotNull() & F.col("event_time").isNotNull()
            & F.col("temperature").between(t_min, t_max)
            & F.col("humidite").between(0.0, 100.0))
    return F.coalesce(cond, F.lit(False))      # une comparaison avec NULL donne NULL : on la force à faux


def mesures_valides(mesures: DataFrame) -> DataFrame:
    return mesures.where(condition_validite()).drop("brut")


def rejets(mesures: DataFrame) -> DataFrame:
    """Messages écartés, avec la raison du rejet (contrôle qualité des données)."""
    raison = (F.when(F.col("sensor_id").isNull() | F.col("event_time").isNull(), "message_illisible")
               .when(F.col("temperature").isNull() | F.col("humidite").isNull(), "valeur_manquante")
               .otherwise("valeur_impossible"))
    return (mesures.where(~condition_validite())
            .select(F.current_timestamp().alias("detection_time"),
                    raison.alias("raison"),
                    "sensor_id", "zone", "temperature", "humidite",
                    F.substring("brut", 1, 200).alias("brut")))


def alertes(valides: DataFrame) -> DataFrame:
    """Filtre SANS ÉTAT : chaque mesure hors plage produit une alerte immédiatement."""
    temp, hum = F.col("temperature"), F.col("humidite")
    hors_plage = (temp > TEMP_MAX) | (temp < TEMP_MIN) | (hum > HUMIDITE_MAX)
    type_alerte = (F.when(temp > TEMP_MAX, "TEMPERATURE_HAUTE")
                    .when(temp < TEMP_MIN, "TEMPERATURE_BASSE")
                    .otherwise("HUMIDITE_HAUTE"))
    niveau = (F.when((temp > TEMP_MAX + MARGE_CRITIQUE) | (temp < TEMP_MIN - MARGE_CRITIQUE), "CRITIQUE")
               .otherwise("ATTENTION"))
    return (valides.where(hors_plage)
            .select("message_id", "sensor_id", "zone", "temperature", "humidite", "event_time",
                    type_alerte.alias("type_alerte"), niveau.alias("niveau"),
                    F.current_timestamp().alias("detection_time")))   # début du micro-lot


def statistiques_fenetres(valides: DataFrame) -> DataFrame:
    """Agrégation AVEC ÉTAT, en temps de l'événement :
       - watermark : on accepte les mesures jusqu'à WATERMARK de retard, puis l'état de la fenêtre est purgé ;
       - dédoublonnage sur message_id (une mesure ne compte qu'une fois) ;
       - fenêtres glissantes de FENETRE recalculées toutes les GLISSEMENT, par chambre froide."""
    stats = (valides
             .withWatermark("event_time", WATERMARK)
             .dropDuplicatesWithinWatermark(["message_id"])
             .groupBy(F.window("event_time", FENETRE, GLISSEMENT).alias("fenetre"), "zone")
             .agg(F.round(F.avg("temperature"), 2).alias("temp_moy"),
                  F.round(F.min("temperature"), 2).alias("temp_min"),
                  F.round(F.max("temperature"), 2).alias("temp_max"),
                  F.round(F.avg("humidite"), 1).alias("humidite_moy"),
                  F.count(F.lit(1)).alias("nb_mesures"),
                  F.approx_count_distinct("sensor_id").alias("nb_capteurs")))
    etat = (F.when((F.col("temp_moy") > TEMP_MAX) | (F.col("temp_moy") < TEMP_MIN), "ALERTE")
             .when((F.col("temp_max") > TEMP_MAX) | (F.col("temp_min") < TEMP_MIN), "VIGILANCE")
             .otherwise("NORMAL"))
    return stats.select(F.col("fenetre.start").alias("debut_fenetre"),
                        F.col("fenetre.end").alias("fin_fenetre"),
                        "zone", "temp_moy", "temp_min", "temp_max", "humidite_moy",
                        "nb_mesures", "nb_capteurs", etat.alias("etat"))


def construire_flux(brut: DataFrame) -> dict:
    """Le graphe de traitement complet, à partir d'un flux brut (colonne `value`)."""
    mesures = parser(brut)
    valides = mesures_valides(mesures)
    return {
        "rejets": rejets(mesures),
        "alertes": alertes(valides),
        "stats": statistiques_fenetres(valides),
        "archive": valides.withColumn("date", F.to_date("event_time")),
    }


# --- Sorties ---------------------------------------------------------------------------------
def serialiser(df: DataFrame, colonne_cle: str) -> DataFrame:
    """Format attendu par le connecteur Kafka : une clé et une valeur (ici du JSON)."""
    return df.select(F.col(colonne_cle).cast("string").alias("key"),
                     F.to_json(F.struct(*df.columns)).alias("value"))


def vers_kafka(df: DataFrame, topic: str, colonne_cle: str, nom: str, declencheur: str, mode: str):
    return (serialiser(df, colonne_cle).writeStream
            .queryName(nom)
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", topic)
            .option("checkpointLocation", f"{CHEMIN_DONNEES}/checkpoints/{nom}")
            .outputMode(mode)
            .trigger(processingTime=declencheur)
            .start())


def vers_parquet(df: DataFrame, nom: str, declencheur: str):
    return (df.writeStream
            .queryName(nom)
            .format("parquet")
            .partitionBy("date", "zone")
            .option("path", f"{CHEMIN_DONNEES}/archive/mesures")
            .option("checkpointLocation", f"{CHEMIN_DONNEES}/checkpoints/{nom}")
            .outputMode("append")
            .trigger(processingTime=declencheur)
            .start())


def demarrer_requetes(flux: dict) -> list:
    return [
        vers_kafka(flux["alertes"], TOPIC_ALERTES, "zone", "alertes", "2 seconds", "append"),
        # update : à chaque micro-lot, on publie les fenêtres qui ont changé (affichage en direct)
        vers_kafka(flux["stats"], TOPIC_STATS, "zone", "stats", "5 seconds", "update"),
        vers_kafka(flux["rejets"], TOPIC_REJETS, "raison", "rejets", "10 seconds", "append"),
        vers_parquet(flux["archive"], "archive", "30 seconds"),
    ]


def afficher_progression(requetes: list):
    for q in requetes:
        p = q.lastProgress
        if not p:
            continue
        duree = p.get("durationMs", {}).get("triggerExecution", 0)
        ligne = (f"[{q.name:8}] lot {p['batchId']:>5} | {p['numInputRows']:>5} messages"
                 f" | {p.get('inputRowsPerSecond', 0):7.1f} msg/s en entrée"
                 f" | {p.get('processedRowsPerSecond', 0):7.1f} msg/s traités | lot traité en {duree} ms")
        filigrane = p.get("eventTime", {}).get("watermark")
        if filigrane and q.name == "stats":
            ligne += f" | watermark {filigrane}"
        print(ligne, flush=True)


def main():
    spark = creer_session()
    flux = construire_flux(lire_kafka(spark))
    requetes = demarrer_requetes(flux)
    print(f"[spark] {len(requetes)} requêtes démarrées. Interface : http://localhost:4040 (onglet Structured Streaming)",
          flush=True)
    try:
        while True:
            # rend la main toutes les 15 s, ou tout de suite si une requête s'arrête sur une erreur
            if spark.streams.awaitAnyTermination(15):
                for q in requetes:
                    if q.exception():
                        raise RuntimeError(f"La requête {q.name} s'est arrêtée : {q.exception()}")
                break
            afficher_progression(requetes)
    except KeyboardInterrupt:
        print("[spark] Arrêt demandé : fermeture propre des requêtes.", flush=True)
        for q in requetes:
            q.stop()


if __name__ == "__main__":
    main()
