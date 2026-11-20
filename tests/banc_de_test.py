"""
Banc de test : exécute le VRAI graphe de traitement de traitement_flux.py (construire_flux, mêmes
requêtes, mêmes modes de sortie, déclencheurs et checkpoints), sans Kafka : la source est un dossier de
fichiers et les sorties Kafka sont remplacées par des fichiers JSONL. Il suffit de pyspark et de Java.

  simulateur --sortie stdout  ->  fichiers JSONL (1 par seconde)  ->  Spark (même code)  ->  sorties JSONL

Usage :
  python tests/banc_de_test.py 240     # 4 minutes de flux (résultats dans tests/run/)
  python tests/verifier.py             # compare les sorties de Spark à un recalcul exact du flux brut
"""
import json
import os
import shutil
import subprocess
import sys
import threading
import time

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = os.path.join(RACINE, "tests", "run")
ENTREE = f"{BASE}/entree"
SORTIES = f"{BASE}/sorties"
BRUT = f"{BASE}/brut.jsonl"
DUREE = int(sys.argv[1]) if len(sys.argv) > 1 else 240

shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(ENTREE); os.makedirs(SORTIES); os.makedirs(f"{BASE}/tmp")

os.environ["CHEMIN_DONNEES"] = f"{BASE}/donnees"
sys.path.insert(0, os.path.join(RACINE, "spark"))
import traitement_flux as tf  # noqa: E402

PRODUCTEUR = None
if os.getenv("BROKER"):  # optionnel : republier les sorties vers un broker Kafka de test
    from confluent_kafka import Producer
    PRODUCTEUR = Producer({"bootstrap.servers": os.getenv("BROKER")})


def vers_kafka_test(df, topic, colonne_cle, nom, declencheur, mode):
    """Remplace le puits Kafka : même sérialisation, même mode de sortie, même déclencheur."""
    chemin = f"{SORTIES}/{topic}.jsonl"

    def ecrire(lot, id_lot):
        lignes = lot.collect()
        with open(chemin, "a") as f:
            for r in lignes:
                f.write(json.dumps({"lot": id_lot, "key": r["key"], "value": json.loads(r["value"]),
                                    "recu": time.time()}) + "\n")
                if PRODUCTEUR:
                    PRODUCTEUR.produce(topic, key=r["key"], value=r["value"])
        if PRODUCTEUR:
            PRODUCTEUR.flush(5)

    return (tf.serialiser(df, colonne_cle).writeStream.queryName(nom)
            .foreachBatch(ecrire)
            .option("checkpointLocation", f"{tf.CHEMIN_DONNEES}/checkpoints/{nom}")
            .outputMode(mode).trigger(processingTime=declencheur).start())


tf.vers_kafka = vers_kafka_test


def alimenter():
    """Lance le simulateur et dépose ses lignes dans un nouveau fichier chaque seconde."""
    sim = subprocess.Popen([sys.executable, os.path.join(RACINE, "simulateur", "simulateur.py"),
                            "--sortie", "stdout", "--duree", str(DUREE), "--graine", "42"],
                           stdout=subprocess.PIPE, stderr=open(f"{BASE}/simulateur.log", "w"), text=True)
    tampon, dernier, n = [], time.time(), 0
    with open(BRUT, "w") as brut:
        for ligne in sim.stdout:
            brut.write(ligne)
            tampon.append(ligne)
            if time.time() - dernier >= 1.0:
                tmp = f"{BASE}/tmp/lot{n:05d}.jsonl"
                with open(tmp, "w") as f:
                    f.writelines(tampon)
                os.rename(tmp, f"{ENTREE}/lot{n:05d}.jsonl")
                tampon, dernier, n = [], time.time(), n + 1
    if tampon:
        with open(f"{ENTREE}/lot{n:05d}.jsonl", "w") as f:
            f.writelines(tampon)


spark = tf.creer_session("test-harnais")
brut = spark.readStream.format("text").option("maxFilesPerTrigger", 50).load(ENTREE)  # colonne `value`
flux = tf.construire_flux(brut)
requetes = tf.demarrer_requetes(flux)
print(f"[harnais] {len(requetes)} requêtes : {[q.name for q in requetes]}", flush=True)

t = threading.Thread(target=alimenter, daemon=True)
t.start()
debut = time.time()
while t.is_alive() or time.time() - debut < DUREE + 45:
    if spark.streams.awaitAnyTermination(15):
        for q in requetes:
            if q.exception():
                print("[harnais] ERREUR", q.name, q.exception(), flush=True)
        break
    tf.afficher_progression(requetes)
    if not t.is_alive() and time.time() - debut > DUREE + 45:
        break
for q in requetes:
    q.stop()
print("[harnais] terminé", flush=True)
