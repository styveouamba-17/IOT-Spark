# Analyse de flux IoT en temps réel avec Spark Structured Streaming

Surveillance d'un entrepôt frigorifique (chaîne du froid) : 12 capteurs simulés envoient une mesure par seconde,
Spark détecte en quelques secondes les chambres froides qui sortent de la plage 2–8 °C, et un tableau de bord
affiche l'état en direct.

```
simulateur ──> Kafka (iot-capteurs) ──> Spark Structured Streaming ──┬──> Kafka iot-alertes ─┐
                                                                     ├──> Kafka iot-stats  ───┼──> tableau de bord
                                                                     ├──> Kafka iot-rejets ───┘    (Streamlit)
                                                                     └──> archive Parquet ──> analyse batch
```

## Contenu

| Dossier / fichier | Rôle |
| --- | --- |
| `simulateur/simulateur.py` | 12 capteurs (3 chambres × 4), anomalies injectées : pannes de froid, valeurs impossibles, JSON tronqué, retards, doublons |
| `spark/traitement_flux.py` | Job de streaming : filtrage, alertes, fenêtres glissantes 30 s / 10 s avec watermark de 30 s et dédoublonnage, archivage Parquet |
| `spark/analyse_batch.py` | Couche batch : relit toute l'archive, retire les doublons, produit une synthèse par chambre (CSV) |
| `dashboard/app.py` | Tableau de bord Streamlit, rafraîchi toutes les 2 s |
| `docker-compose.yml` | Kafka 4.1.2 (KRaft), création des topics, simulateur, Spark 4.1.2, tableau de bord |
| `tests/` | Banc de test sans Kafka et script de vérification des résultats |
| `docs/capture_tableau_de_bord.png` | Capture du tableau de bord pendant une panne simulée |

## Lancer le projet (Docker)

Prérequis : Docker Desktop (ou Docker Engine + Compose), environ 4 Go de RAM libres, une connexion Internet au
premier lancement (images Docker et connecteur Kafka de Spark).

```bash
docker compose up -d --build
```

Attendre environ une minute, puis ouvrir :

- tableau de bord : <http://localhost:8501> ;
- interface Spark : <http://localhost:4040>, onglet **Structured Streaming** (débit, durée des micro-lots, watermark).

La première panne de froid est simulée dans `chambre-B` environ 40 secondes après le démarrage du simulateur :
la température monte pendant 90 s puis redescend en 60 s. Ensuite, une panne survient en moyenne toutes les 4 minutes.

### Commandes utiles

```bash
docker compose ps                                   # état des services
docker compose logs -f spark                        # progression des micro-lots (toutes les 15 s)
docker compose logs -f simulateur                   # bilan des messages et anomalies envoyés

# lire un topic Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic iot-alertes --from-beginning --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic iot-capteurs

# couche batch : synthèse de toutes les mesures archivées
docker compose exec spark /opt/spark/bin/spark-submit /app/analyse_batch.py

# tester la reprise après panne : arrêter Spark, attendre, relancer
docker compose stop spark && sleep 30 && docker compose start spark

docker compose down        # arrêter (les données sont conservées)
docker compose down -v     # arrêter et tout effacer (topics, checkpoints, archive)
```

## Lancer sans Docker pour Spark (mise au point)

Avec un Kafka déjà accessible sur `localhost:9092` (par exemple le service `kafka` du compose seul :
`docker compose up -d kafka kafka-init`) et Java 17 ou 21 :

```bash
pip install pyspark==4.1.2 confluent-kafka==2.16.1 streamlit==1.67.0 pandas

python simulateur/simulateur.py                       # terminal 1
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 spark/traitement_flux.py   # terminal 2
streamlit run dashboard/app.py                        # terminal 3

python simulateur/simulateur.py --sortie stdout       # voir les messages JSON sans Kafka
```

Sous Windows, Spark a besoin de `winutils.exe` et de `HADOOP_HOME` pour écrire ses checkpoints : préférer Docker.

## Tester sans Kafka

Le banc de test exécute le même graphe de traitement Spark sur un flux enregistré du simulateur (source fichiers),
puis compare les rejets, les alertes et chaque fenêtre à un recalcul exact du flux brut. Il suffit de
`pip install pyspark==4.1.2` et de Java 17 ou 21.

```bash
python tests/banc_de_test.py 240      # 4 minutes de flux ; sorties dans tests/run/
python tests/verifier.py              # rejets, alertes, fenêtres, doublons, latence
```

## Paramètres (variables d'environnement)

| Variable | Défaut | Utilisée par |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` (`kafka:19092` dans Docker) | tous |
| `INTERVALLE_S` | `1.0` | simulateur : secondes entre deux mesures d'un capteur |
| `GRAINE` | aléatoire | simulateur : scénario reproductible |
| `FENETRE` / `GLISSEMENT` / `WATERMARK` | `30 seconds` / `10 seconds` / `30 seconds` | Spark |
| `CHEMIN_DONNEES` | `./donnees` (`/donnees` dans Docker) | Spark : archive Parquet, checkpoints, rapports |
| `SHUFFLE_PARTITIONS` | `4` | Spark : partitions des agrégations (fixé dans le checkpoint au 1er lancement) |
| `HISTORIQUE_MIN` | `15` | tableau de bord : minutes affichées sur le graphique |

Changer `FENETRE`, `GLISSEMENT`, `WATERMARK` ou `SHUFFLE_PARTITIONS` modifie l'état de la requête : il faut alors
repartir de zéro (`docker compose down -v`).

## Format des messages

Entrée (`iot-capteurs`, clé = identifiant du capteur, 3 partitions) :

```json
{"message_id": "6fa17735-…", "sensor_id": "capteur-B2", "zone": "chambre-B",
 "temperature": 5.47, "humidite": 70.3, "event_time": "2026-11-17T09:30:00.123Z"}
```

Sorties : `iot-alertes` (une alerte par mesure hors plage, niveau `ATTENTION` ou `CRITIQUE` au-delà de 10 °C ou
en dessous de 0 °C), `iot-stats` (une ligne par chambre et par fenêtre, mise à jour à chaque micro-lot, état
`NORMAL` / `VIGILANCE` / `ALERTE`), `iot-rejets` (message écarté et raison du rejet).

## Dépannage

- **Le tableau de bord reste sur « En attente… »** : Spark télécharge le connecteur Kafka au premier lancement
  (`docker compose logs spark`). Les premières fenêtres arrivent environ 30 s après le démarrage du job.
- **`spark` redémarre en boucle** : vérifier la connexion Internet (téléchargement du connecteur), ou un checkpoint
  incompatible après un changement de paramètres (`docker compose down -v`).
- **Ports déjà utilisés** (9092, 4040, 8501) : arrêter le service local qui les occupe ou changer le port à gauche
  dans `docker-compose.yml`.
