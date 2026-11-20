"""
Simulateur de capteurs IoT : entrepôt frigorifique (chaîne du froid).

Chaque capteur envoie une mesure JSON par seconde dans le topic Kafka `iot-capteurs` :

    {"message_id": "…", "sensor_id": "capteur-B2", "zone": "chambre-B",
     "temperature": 4.37, "humidite": 71.2, "event_time": "2026-11-17T09:30:00.123Z"}

Pour tester le traitement Spark, le simulateur injecte volontairement :
  - des pannes de froid : la température d'une chambre dérive vers le haut puis revient ;
  - des valeurs aberrantes (capteur défectueux) et des messages JSON illisibles ;
  - des messages en retard : le capteur garde une mesure (coupure réseau) et l'envoie 5 à 25 s plus tard,
    parfois 60 à 120 s plus tard ; son temps de l'événement reste celui de la mesure ;
  - des doublons : le même message (même message_id) est envoyé deux fois.

Usage :
    python simulateur.py                          # vers Kafka (KAFKA_BOOTSTRAP, défaut localhost:9092)
    python simulateur.py --sortie stdout          # affiche le JSON, sans Kafka (mise au point)
    python simulateur.py --duree 300 --graine 42  # 5 minutes, scénario reproductible
"""

import argparse
import json
import math
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

# --- Configuration de l'entrepôt ---------------------------------------------------------
ZONES = {                      # zone -> consigne de température (°C), plage autorisée : 2 à 8 °C
    "chambre-A": 4.5,
    "chambre-B": 5.5,
    "chambre-C": 3.8,
}
CAPTEURS_PAR_ZONE = 4
HUMIDITE_NORMALE = 70.0        # % d'humidité relative

# Probabilités d'anomalies, par message
P_ABERRANT = 0.005             # capteur défectueux : valeur impossible ou manquante
P_ILLISIBLE = 0.003            # message JSON tronqué
P_RETARD = 0.03                # message en retard de 5 à 25 s (accepté par le watermark de 30 s)
P_TRES_EN_RETARD = 0.005       # message en retard de 60 à 120 s (trop tard pour les fenêtres)
P_DOUBLON = 0.02               # même message envoyé deux fois

# Pannes de froid
PREMIERE_PANNE_APRES_S = 40    # première panne rapide, pour la démonstration
P_PANNE_PAR_SECONDE = 1 / 240  # ensuite, en moyenne une panne toutes les 4 minutes
DUREE_MONTEE_S = 90            # la température monte pendant 90 s…
DUREE_RETOUR_S = 60            # … puis redescend en 60 s
PENTE_C_PAR_S = 0.08           # +0,08 °C par seconde : +7,2 °C au bout de 90 s


@dataclass
class Panne:
    zone: str
    debut: float               # instant de début (secondes depuis le lancement)

    def ecart(self, t: float) -> float:
        """Écart de température (°C) dû à la panne à l'instant t."""
        dt = t - self.debut
        if dt < 0:
            return 0.0
        if dt <= DUREE_MONTEE_S:
            return PENTE_C_PAR_S * dt
        retour = dt - DUREE_MONTEE_S
        if retour <= DUREE_RETOUR_S:
            return PENTE_C_PAR_S * DUREE_MONTEE_S * (1 - retour / DUREE_RETOUR_S)
        return 0.0

    def terminee(self, t: float) -> bool:
        return t - self.debut > DUREE_MONTEE_S + DUREE_RETOUR_S


def capteurs():
    """Liste des capteurs : (sensor_id, zone)."""
    return [(f"capteur-{zone[-1]}{i}", zone)
            for zone in ZONES for i in range(1, CAPTEURS_PAR_ZONE + 1)]


def horodatage(dt: datetime) -> str:
    """ISO 8601 en UTC avec millisecondes, par ex. 2026-11-17T09:30:00.123Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class Simulateur:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.debut = time.time()
        self.pannes: list[Panne] = []
        self.en_attente: list[tuple] = []   # (instant d'envoi, clé, valeur) : mesures retenues par un capteur
        self.premiere_panne_lancee = False
        self.compteurs = {k: 0 for k in ("envoyes", "aberrants", "illisibles", "retards",
                                          "tres_en_retard", "doublons", "pannes")}
        # Petit décalage propre à chaque capteur (deux capteurs ne mesurent jamais exactement pareil)
        self.biais = {sid: rng.uniform(-0.3, 0.3) for sid, _ in capteurs()}

    # --- Pannes -----------------------------------------------------------------------------
    def mettre_a_jour_pannes(self, t: float):
        self.pannes = [p for p in self.pannes if not p.terminee(t)]
        zones_en_panne = {p.zone for p in self.pannes}
        if not self.premiere_panne_lancee and t >= PREMIERE_PANNE_APRES_S:
            self.premiere_panne_lancee = True
            self._lancer_panne("chambre-B", t)
        elif self.premiere_panne_lancee and self.rng.random() < P_PANNE_PAR_SECONDE:
            libres = [z for z in ZONES if z not in zones_en_panne]
            if libres:
                self._lancer_panne(self.rng.choice(libres), t)

    def _lancer_panne(self, zone: str, t: float):
        self.pannes.append(Panne(zone, t))
        self.compteurs["pannes"] += 1
        print(f"[simulateur] Panne de froid simulée dans {zone} à t={t:.0f}s", file=sys.stderr, flush=True)

    def ecart_panne(self, zone: str, t: float) -> float:
        return sum(p.ecart(t) for p in self.pannes if p.zone == zone)

    # --- Mesures ----------------------------------------------------------------------------
    def mesure(self, sensor_id: str, zone: str, t: float, maintenant: datetime) -> dict:
        consigne = ZONES[zone]
        ecart = self.ecart_panne(zone, t)
        # cycle du compresseur (période 60 s) + bruit de mesure + panne éventuelle
        temperature = (consigne + self.biais[sensor_id]
                       + 0.5 * math.sin(2 * math.pi * t / 60)
                       + self.rng.gauss(0, 0.15) + ecart)
        # quand la porte est ouverte (panne), l'air humide entre : l'humidité monte aussi
        humidite = HUMIDITE_NORMALE + self.rng.gauss(0, 1.5) + 2.5 * ecart
        return {
            "message_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "sensor_id": sensor_id,
            "zone": zone,
            "temperature": round(temperature, 2),
            "humidite": round(min(humidite, 100.0), 1),
            "event_time": horodatage(maintenant),
        }

    def messages_a_envoyer(self, t: float, maintenant: datetime):
        """Génère les messages d'un tour (une mesure par capteur), anomalies comprises.
        Renvoie une liste de (clé, valeur en octets)."""
        sortie = []
        for sensor_id, zone in capteurs():
            m = self.mesure(sensor_id, zone, t, maintenant)
            r = self.rng.random()
            retard = 0.0
            if r < P_ABERRANT:                                   # capteur défectueux
                m["temperature"] = self.rng.choice([-99.9, 150.0, None])
                self.compteurs["aberrants"] += 1
            elif r < P_ABERRANT + P_RETARD:                      # coupure courte : retard acceptable
                retard = self.rng.uniform(5, 25)
                self.compteurs["retards"] += 1
            elif r < P_ABERRANT + P_RETARD + P_TRES_EN_RETARD:   # coupure longue : retard trop important
                retard = self.rng.uniform(60, 120)
                self.compteurs["tres_en_retard"] += 1

            valeur = json.dumps(m, ensure_ascii=False).encode("utf-8")
            if self.rng.random() < P_ILLISIBLE:                 # message tronqué en cours d'envoi
                valeur = valeur[: self.rng.randint(5, len(valeur) - 5)]
                self.compteurs["illisibles"] += 1
            if retard:
                # le capteur garde la mesure (avec son vrai temps de l'événement) et l'envoie plus tard
                self.en_attente.append((t + retard, sensor_id, valeur))
                continue
            sortie.append((sensor_id, valeur))
            if self.rng.random() < P_DOUBLON:                   # renvoi du même message
                sortie.append((sensor_id, valeur))
                self.compteurs["doublons"] += 1

        # mesures retenues dont le délai est écoulé : elles arrivent maintenant, en retard
        prets = [x for x in self.en_attente if x[0] <= t]
        self.en_attente = [x for x in self.en_attente if x[0] > t]
        sortie.extend((cle, valeur) for _, cle, valeur in prets)
        return sortie


# --- Sorties ----------------------------------------------------------------------------------
class SortieKafka:
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer  # importé ici : le mode stdout n'a pas besoin de Kafka
        self.topic = topic
        self.producteur = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "simulateur-iot",
            "acks": "all",
            "enable.idempotence": True,   # pas de doublons dus aux renvois automatiques du client
            "linger.ms": 20,
        })
        self.erreurs = 0

    def _rapport(self, err, msg):
        if err is not None:
            self.erreurs += 1
            print(f"[simulateur] Échec de livraison : {err}", file=sys.stderr, flush=True)

    def envoyer(self, cle: str, valeur: bytes):
        while True:
            try:
                self.producteur.produce(self.topic, key=cle.encode(), value=valeur, on_delivery=self._rapport)
                break
            except BufferError:               # file d'attente locale pleine : on laisse Kafka respirer
                self.producteur.poll(0.5)
        self.producteur.poll(0)

    def fermer(self):
        self.producteur.flush(10)


class SortieStdout:
    def envoyer(self, cle: str, valeur: bytes):
        sys.stdout.write(valeur.decode("utf-8", errors="replace") + "\n")
        sys.stdout.flush()

    def fermer(self):
        pass


def main():
    p = argparse.ArgumentParser(description="Simulateur de capteurs IoT (chaîne du froid)")
    p.add_argument("--sortie", choices=["kafka", "stdout"], default=os.getenv("SORTIE", "kafka"))
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("TOPIC_CAPTEURS", "iot-capteurs"))
    p.add_argument("--intervalle", type=float, default=float(os.getenv("INTERVALLE_S", "1.0")),
                   help="secondes entre deux mesures d'un même capteur (défaut 1)")
    p.add_argument("--duree", type=float, default=float(os.getenv("DUREE_S", "0")),
                   help="durée de la simulation en secondes (0 = sans fin)")
    p.add_argument("--graine", type=int, default=int(os.getenv("GRAINE", "0")) or None,
                   help="graine aléatoire pour un scénario reproductible")
    args = p.parse_args()

    rng = random.Random(args.graine)
    sim = Simulateur(rng)
    sortie = SortieKafka(args.bootstrap, args.topic) if args.sortie == "kafka" else SortieStdout()
    print(f"[simulateur] {len(capteurs())} capteurs, 1 mesure / {args.intervalle}s, sortie={args.sortie}"
          + (f" ({args.bootstrap}, topic {args.topic})" if args.sortie == "kafka" else ""),
          file=sys.stderr, flush=True)

    prochain_tour = time.time()
    dernier_bilan = time.time()
    try:
        while True:
            t = time.time() - sim.debut
            if args.duree and t >= args.duree:
                break
            sim.mettre_a_jour_pannes(t)
            for cle, valeur in sim.messages_a_envoyer(t, datetime.now(timezone.utc)):
                sortie.envoyer(cle, valeur)
                sim.compteurs["envoyes"] += 1
            if time.time() - dernier_bilan >= 10:
                print(f"[simulateur] t={t:.0f}s bilan : {sim.compteurs}", file=sys.stderr, flush=True)
                dernier_bilan = time.time()
            prochain_tour += args.intervalle
            time.sleep(max(0.0, prochain_tour - time.time()))
    except KeyboardInterrupt:
        pass
    finally:
        sortie.fermer()
        print(f"[simulateur] Arrêt. Bilan final : {sim.compteurs}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
