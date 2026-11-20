"""Vérifie les sorties du harnais contre la vérité terrain (le flux brut produit par le simulateur)."""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
import statistics

RUN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "run")


def iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# --- Vérité terrain ---------------------------------------------------------------------------
lignes = open(f"{RUN}/brut.jsonl").read().splitlines()
valides, invalides = [], 0
for l in lignes:
    try:
        m = json.loads(l)
    except ValueError:
        invalides += 1
        continue
    ok = all(m.get(k) is not None for k in ("message_id", "sensor_id", "zone", "event_time", "temperature", "humidite"))
    ok = ok and -40 <= m["temperature"] <= 60 and 0 <= m["humidite"] <= 100
    if ok:
        valides.append(m)
    else:
        invalides += 1
uniques = {m["message_id"]: m for m in valides}
print(f"Flux brut : {len(lignes)} messages, {len(valides)} valides, {invalides} invalides, "
      f"{len(valides) - len(uniques)} doublons valides")

# --- Rejets -------------------------------------------------------------------------------------
rej = [json.loads(l) for l in open(f"{RUN}/sorties/iot-rejets.jsonl")]
print(f"Rejets Spark : {len(rej)} (attendu {invalides}) ->", "OK" if len(rej) == invalides else "ÉCART")
print("   par raison :", dict(__import__("collections").Counter(r["value"]["raison"] for r in rej)))

# --- Alertes ------------------------------------------------------------------------------------
attendues = [m for m in valides if m["temperature"] > 8 or m["temperature"] < 2 or m["humidite"] > 85]
al = [json.loads(l) for l in open(f"{RUN}/sorties/iot-alertes.jsonl")]
print(f"Alertes Spark : {len(al)} (attendu {len(attendues)}) ->", "OK" if len(al) == len(attendues) else "ÉCART")
print("   par type :", dict(__import__("collections").Counter(a["value"]["type_alerte"] for a in al)),
      "| par niveau :", dict(__import__("collections").Counter(a["value"]["niveau"] for a in al)))
lat_det = [(iso(a["value"]["detection_time"]) - iso(a["value"]["event_time"])).total_seconds() for a in al]
lat_sortie = [a["recu"] - iso(a["value"]["event_time"]).timestamp() for a in al]
print(f"   latence événement -> début du micro-lot : médiane {statistics.median(lat_det):.2f} s")
print(f"   latence événement -> écriture de la sortie : médiane {statistics.median(lat_sortie):.2f} s, "
      f"p90 {sorted(lat_sortie)[int(0.9 * len(lat_sortie))]:.2f} s")
zones_alerte = sorted({a["value"]["zone"] for a in al})
t_al = sorted(iso(a["value"]["event_time"]) for a in al)
print(f"   zones en alerte : {zones_alerte} ; première alerte {t_al[0]:%H:%M:%S}, dernière {t_al[-1]:%H:%M:%S}")

# --- Fenêtres -----------------------------------------------------------------------------------
stats = [json.loads(l)["value"] for l in open(f"{RUN}/sorties/iot-stats.jsonl")]
finales = {}
versions = defaultdict(int)
for s in stats:
    finales[(s["zone"], s["debut_fenetre"])] = s
    versions[(s["zone"], s["debut_fenetre"])] += 1

# vérité terrain par fenêtre (30 s, glissement 10 s, alignées sur l'époque), sans doublons
vt = defaultdict(list)
for m in uniques.values():
    t = iso(m["event_time"]).timestamp()
    base = int(t // 10) * 10
    for debut in (base - 20, base - 10, base):
        if debut <= t < debut + 30:
            vt[(m["zone"], datetime.fromtimestamp(debut, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"))].append(m)

# mesures arrivées après le watermark (plus anciennes que le max déjà vu − 30 s, dans l'ordre d'arrivée)
max_vu, trop_tard = None, set()
for l in lignes:
    try:
        m = json.loads(l)
        t = iso(m["event_time"]).timestamp()
    except (ValueError, KeyError, TypeError, AttributeError):
        continue
    if max_vu is not None and t < max_vu - 30:
        trop_tard.add(m.get("message_id"))
    max_vu = t if max_vu is None else max(max_vu, t)
print(f"Mesures arrivées après le watermark (ignorées par les fenêtres) : {len(trop_tard)}")
vt2 = {k: [m for m in v if m["message_id"] not in trop_tard] for k, v in vt.items()}
id2 = 0
for cle, s in finales.items():
    ms = vt2.get(cle, [])
    moy = round(sum(x["temperature"] for x in ms) / len(ms), 2) if ms else None
    id2 += (len(ms) == s["nb_mesures"] and moy == s["temp_moy"])
print(f"   fenêtres identiques à la vérité terrain dédoublonnée, sans les mesures trop en retard : {id2}/{len(finales)}")

identiques, ecarts = 0, []
for cle, s in finales.items():
    ms = vt.get(cle, [])
    moy = round(sum(x["temperature"] for x in ms) / len(ms), 2) if ms else None
    if len(ms) == s["nb_mesures"] and moy == s["temp_moy"]:
        identiques += 1
    else:
        ecarts.append((cle, s["nb_mesures"], len(ms), s["temp_moy"], moy))
print(f"Fenêtres publiées : {len(finales)} (en moyenne {sum(versions.values()) / len(versions):.1f} mises à jour par fenêtre)")
print(f"   identiques à la vérité terrain dédoublonnée : {identiques}/{len(finales)}")
for e in ecarts[:12]:
    print("   écart", e)
trop = [e for e in ecarts if e[1] > e[2]]
print(f"   fenêtres avec PLUS de mesures que la vérité dédoublonnée (doublons comptés) : {len(trop)}")
alerte = sorted({(k[0], v['debut_fenetre'][11:19]) for k, v in finales.items() if v["etat"] == "ALERTE"})
print(f"   fenêtres en état ALERTE : {len(alerte)} -> {alerte[:3]} … {alerte[-2:]}")
vig = [k for k, v in finales.items() if v["etat"] == "VIGILANCE"]
print(f"   fenêtres en état VIGILANCE : {len(vig)}")
maxt = max(finales.values(), key=lambda v: v["temp_moy"])
print(f"   moyenne maximale : {maxt['temp_moy']} °C ({maxt['zone']}, {maxt['debut_fenetre'][11:19]}–{maxt['fin_fenetre'][11:19]})")
