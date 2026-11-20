"""
Tableau de bord temps réel (Streamlit) de l'entrepôt frigorifique.

Il lit dans Kafka les trois topics produits par le job Spark :
  iot-stats    agrégats par chambre froide sur fenêtres glissantes (mode update : la dernière version gagne)
  iot-alertes  une alerte par mesure hors plage
  iot-rejets   messages écartés par le contrôle qualité

Un fil d'exécution en arrière-plan consomme les messages et les garde en mémoire ;
la page se redessine toutes les 2 secondes.

Lancement : streamlit run app.py   (KAFKA_BOOTSTRAP, défaut localhost:9092)
"""

import json
import os
import threading
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_STATS = os.getenv("TOPIC_STATS", "iot-stats")
TOPIC_ALERTES = os.getenv("TOPIC_ALERTES", "iot-alertes")
TOPIC_REJETS = os.getenv("TOPIC_REJETS", "iot-rejets")
TEMP_MIN, TEMP_MAX = 2.0, 8.0
HISTORIQUE = timedelta(minutes=int(os.getenv("HISTORIQUE_MIN", "15")))   # profondeur du graphique
LIBELLES_ETAT = {"NORMAL": ("Normal", "green"), "VIGILANCE": ("Vigilance", "orange"), "ALERTE": ("Alerte", "red")}


def date_iso(texte):
    """'2026-11-17T09:30:00.123Z' -> datetime UTC (None si absent ou illisible)."""
    try:
        return datetime.fromisoformat(texte.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


class EtatFlux:
    """Données reçues de Kafka, partagées entre le fil consommateur et la page."""

    def __init__(self):
        self.verrou = threading.Lock()
        self.fenetres = {}                    # (zone, debut_fenetre) -> dernière version de la fenêtre
        self.alertes = deque(maxlen=2000)
        self.rejets = deque(maxlen=300)
        self.rejets_par_raison = Counter()
        self.recus = Counter()                # nombre de messages reçus par topic
        self.derniere_reception = None
        self.erreur = None

    def ajouter(self, topic, valeur, recu):
        with self.verrou:
            self.recus[topic] += 1
            self.derniere_reception = recu
            if topic == TOPIC_STATS:
                cle = (valeur.get("zone"), valeur.get("debut_fenetre"))
                self.fenetres[cle] = valeur
            elif topic == TOPIC_ALERTES:
                valeur["recu"] = recu
                self.alertes.append(valeur)
            elif topic == TOPIC_REJETS:
                self.rejets.append(valeur)
                self.rejets_par_raison[valeur.get("raison", "inconnue")] += 1

    def purger(self, maintenant):
        """Oublie les fenêtres trop anciennes pour le graphique (la mémoire ne grossit pas sans fin)."""
        limite = maintenant - HISTORIQUE - timedelta(minutes=1)
        with self.verrou:
            for cle in [c for c, v in self.fenetres.items()
                        if (date_iso(v.get("fin_fenetre")) or maintenant) < limite]:
                del self.fenetres[cle]

    def instantane(self):
        with self.verrou:
            return (list(self.fenetres.values()), list(self.alertes), list(self.rejets),
                    dict(self.rejets_par_raison), dict(self.recus), self.derniere_reception, self.erreur)


def consommer(etat: EtatFlux):
    from confluent_kafka import Consumer
    while True:                      # reconnexion si Kafka n'est pas encore prêt
        try:
            conso = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": f"tableau-de-bord-{uuid.uuid4().hex[:8]}",   # chaque page relit tout l'historique
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            conso.subscribe([TOPIC_STATS, TOPIC_ALERTES, TOPIC_REJETS])
            while True:
                for msg in conso.consume(num_messages=500, timeout=1.0):
                    if msg.error():
                        etat.erreur = str(msg.error())
                        continue
                    try:
                        valeur = json.loads(msg.value())
                    except (TypeError, ValueError):
                        continue
                    etat.erreur = None
                    etat.ajouter(msg.topic(), valeur, datetime.now(timezone.utc))
        except Exception as exc:     # noqa: BLE001 - on affiche l'erreur et on réessaie
            etat.erreur = f"{type(exc).__name__} : {exc}"
            time.sleep(3)


@st.cache_resource
def demarrer_consommateur() -> EtatFlux:
    etat = EtatFlux()
    threading.Thread(target=consommer, args=(etat,), daemon=True, name="consommateur-kafka").start()
    return etat


# --- Préparation des données affichées ------------------------------------------------------
def tableau_fenetres(fenetres):
    if not fenetres:
        return pd.DataFrame()
    df = pd.DataFrame(fenetres)
    df["debut_fenetre"] = pd.to_datetime(df["debut_fenetre"], utc=True)
    df["fin_fenetre"] = pd.to_datetime(df["fin_fenetre"], utc=True)
    return df.sort_values("fin_fenetre")


def derniere_fenetre_complete(df, zone, maintenant):
    """La fenêtre la plus récente déjà terminée (les fenêtres en cours ne contiennent qu'une partie des mesures)."""
    d = df[(df["zone"] == zone) & (df["fin_fenetre"] <= maintenant)]
    if d.empty:
        return None, None
    d = d.sort_values("fin_fenetre")
    precedente = d.iloc[-2] if len(d) > 1 else None
    return d.iloc[-1], precedente


def latence_mediane(alertes, maintenant):
    """Latence de bout en bout (capteur -> écran) des alertes reçues en direct ces 5 dernières minutes."""
    valeurs = []
    for a in alertes:
        ev, det, recu = date_iso(a.get("event_time")), date_iso(a.get("detection_time")), a.get("recu")
        if not (ev and det and recu) or maintenant - recu > timedelta(minutes=5):
            continue
        if (recu - det).total_seconds() > 30:     # alerte relue depuis l'historique au démarrage : ignorée
            continue
        valeurs.append((recu - ev).total_seconds())
    return pd.Series(valeurs).median() if valeurs else None


def graphique_temperatures(df, maintenant):
    d = df[df["fin_fenetre"] >= maintenant - HISTORIQUE]
    d = d[d["fin_fenetre"] <= maintenant]                       # fenêtres terminées uniquement
    base = alt.Chart(d).encode(
        x=alt.X("fin_fenetre:T", title="Fin de la fenêtre (UTC)", scale=alt.Scale(type="utc"),
                axis=alt.Axis(format="%H:%M:%S")),
        # couleurs neutres pour les chambres : le rouge est réservé aux seuils
        color=alt.Color("zone:N", title="Chambre",
                        scale=alt.Scale(range=["#4c78a8", "#f58518", "#54a24b", "#b279a2", "#9d755d"])),
    )
    lignes = base.mark_line(point=alt.OverlayMarkDef(size=18)).encode(
        y=alt.Y("temp_moy:Q", title="Température moyenne (°C)", scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("zone:N", title="Chambre"),
                 alt.Tooltip("debut_fenetre:T", title="Début", format="%H:%M:%S", formatType="utc"),
                 alt.Tooltip("fin_fenetre:T", title="Fin", format="%H:%M:%S", formatType="utc"),
                 alt.Tooltip("temp_moy:Q", title="Moyenne (°C)"),
                 alt.Tooltip("temp_max:Q", title="Max (°C)"),
                 alt.Tooltip("nb_mesures:Q", title="Mesures"),
                 alt.Tooltip("etat:N", title="État")],
    )
    df_seuils = pd.DataFrame({"seuil": [TEMP_MIN, TEMP_MAX],
                              "libelle": [f"min {TEMP_MIN:g} °C", f"max {TEMP_MAX:g} °C"]})
    regles = alt.Chart(df_seuils).mark_rule(strokeDash=[6, 4], color="#d62728", strokeWidth=1.5).encode(y="seuil:Q")
    libelles = alt.Chart(df_seuils).mark_text(align="left", dx=4, dy=-6, color="#d62728", fontSize=11).encode(
        y="seuil:Q", text="libelle:N", x=alt.value(0))
    return (regles + libelles + lignes).properties(height=340)


# --- Page ------------------------------------------------------------------------------------
st.set_page_config(page_title="Chaîne du froid : flux IoT", page_icon="❄️", layout="wide")
st.title("Surveillance de l'entrepôt frigorifique en temps réel")
st.caption("Capteurs simulés → Kafka → Spark Structured Streaming → Kafka → ce tableau de bord. "
           f"Plage autorisée : {TEMP_MIN:g} à {TEMP_MAX:g} °C. Heures en UTC.")

etat = demarrer_consommateur()


@st.fragment(run_every=2)
def rafraichir():
    maintenant = datetime.now(timezone.utc)
    etat.purger(maintenant)
    fenetres, alertes, rejets, par_raison, recus, derniere, erreur = etat.instantane()
    df = tableau_fenetres(fenetres)

    if erreur:
        st.warning(f"Kafka ({KAFKA_BOOTSTRAP}) : {erreur}")
    if df.empty:
        st.info("En attente des premières fenêtres calculées par Spark (environ 30 secondes après le démarrage)…")
        etat_connexion(recus)
        return

    # 1. État de chaque chambre froide (dernière fenêtre complète)
    zones = sorted(df["zone"].dropna().unique())
    for col, zone in zip(st.columns(len(zones)), zones):
        derniere_f, precedente = derniere_fenetre_complete(df, zone, maintenant)
        with col.container(border=True):
            if derniere_f is None:
                st.metric(zone, "…")
                continue
            delta = None if precedente is None else f"{derniere_f['temp_moy'] - precedente['temp_moy']:+.2f} °C"
            st.metric(zone, f"{derniere_f['temp_moy']:.1f} °C", delta, delta_color="inverse",
                      help="Moyenne de la dernière fenêtre de 30 s terminée, écart avec la précédente")
            libelle, couleur = LIBELLES_ETAT.get(derniere_f["etat"], (derniere_f["etat"], "gray"))
            st.badge(libelle, color=couleur)
            st.caption(f"Humidité {derniere_f['humidite_moy']:.0f} % · {int(derniere_f['nb_capteurs'])} capteurs · "
                       f"{int(derniere_f['nb_mesures'])} mesures · fenêtre "
                       f"{derniere_f['debut_fenetre']:%H:%M:%S}–{derniere_f['fin_fenetre']:%H:%M:%S}")

    # 2. Indicateurs
    recentes = [a for a in alertes if (date_iso(a.get("event_time")) or maintenant) >= maintenant - timedelta(minutes=5)]
    latence = latence_mediane(alertes, maintenant)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Alertes (5 dernières minutes)", len(recentes),
              help="Une alerte par mesure hors plage, émise par le filtre sans état de Spark")
    k2.metric("Mesures rejetées (total)", sum(par_raison.values()),
              help="Messages illisibles, valeurs manquantes ou physiquement impossibles")
    k3.metric("Latence médiane capteur → écran", "–" if latence is None else f"{latence:.1f} s",
              help="Temps de traitement − temps de l'événement, pour les alertes reçues en direct")
    k4.metric("Dernière donnée reçue", "–" if derniere is None else f"il y a {(maintenant - derniere).total_seconds():.0f} s")

    # 3. Températures moyennes par fenêtre glissante
    st.subheader("Température moyenne par chambre (fenêtres glissantes de 30 s, toutes les 10 s)")
    st.altair_chart(graphique_temperatures(df, maintenant), width="stretch")

    # 4. Alertes et rejets
    gauche, droite = st.columns([3, 2])
    with gauche:
        st.subheader("Dernières alertes")
        if alertes:
            da = pd.DataFrame(alertes[-200:][::-1])
            da["heure"] = pd.to_datetime(da["event_time"], utc=True).dt.strftime("%H:%M:%S")
            st.dataframe(da[["heure", "sensor_id", "zone", "type_alerte", "niveau", "temperature", "humidite"]]
                         .rename(columns={"sensor_id": "capteur", "zone": "chambre", "type_alerte": "type",
                                          "temperature": "temp. (°C)", "humidite": "humidité (%)"}),
                         hide_index=True, height=320)
        else:
            st.write("Aucune alerte pour l'instant.")
    with droite:
        st.subheader("Contrôle qualité")
        if par_raison:
            st.dataframe(pd.DataFrame(sorted(par_raison.items(), key=lambda kv: -kv[1]),
                                      columns=["raison du rejet", "messages"]), hide_index=True)
            dr = pd.DataFrame(rejets[-20:][::-1])
            colonnes = [c for c in ["raison", "sensor_id", "brut"] if c in dr.columns]
            st.dataframe(dr[colonnes], hide_index=True, height=200)
        else:
            st.write("Aucun rejet pour l'instant.")

    etat_connexion(recus)


def etat_connexion(recus):
    # écrit DANS le fragment (une écriture dans st.sidebar depuis un fragment est refusée par Streamlit)
    st.caption(f"Kafka `{KAFKA_BOOTSTRAP}` · messages reçus : "
               + " · ".join(f"`{t}` {recus.get(t, 0)}" for t in (TOPIC_STATS, TOPIC_ALERTES, TOPIC_REJETS)))


rafraichir()
