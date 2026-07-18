"""
Configuration et fonctions partagées entre les caméras.

Ce module représente le code IDENTIQUE qui serait packagé et déployé sur chaque
unité Jetson Xavier NX du stade. Il ne contient aucune logique de coordination
inter-caméras : chaque caméra, une fois lancée via camera_node.py, fonctionne en
toute indépendance et publie sur son propre topic Kafka.

La corrélation cross-caméra (CAMERA_ADJACENCY, MERGEABLE_EVENT_TYPES ci-dessous)
n'est PAS exécutée à ce stade. Elle est fournie ici pour être réutilisée plus tard
par un consommateur Kafka en aval (ex. job Spark Structured Streaming) qui lira
les 8 topics et effectuera la déduplication après coup.
"""

import json
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


# ----------------------------------------------------------------------------
# 1. Configuration des caméras (déploiement en boucle périmétrique, 8 unités)
# ----------------------------------------------------------------------------

@dataclass
class Camera:
    camera_id: str
    zone: str                  # zone du terrain couverte
    zone_type: str             # "corner" | "midline" | "goal" -> pondère les types d'événements
    device: str = "Jetson Xavier NX"
    model: str = "YOLOv8m-TensorRT-INT8"
    fps: int = 30
    event_probability: float = 0.35   # probabilité de détection à chaque tick
    topic: str = ""                     # topic Kafka dédié à cette caméra (calculé si vide)

    def __post_init__(self):
        if not self.topic:
            self.topic = f"match-events.{self.camera_id.lower()}"


# C1 -> C2 -> C3 (tribune principale : corner / milieu / corner)
# -> C4 (cage droite)
# -> C5 -> C6 -> C7 (tribune opposée : corner / milieu / corner)
# -> C8 (cage gauche) -> retour à C1
CAMERAS = [
    Camera(camera_id="CAM-01", zone="corner_tribune_principale_gauche",
           zone_type="corner", event_probability=0.30),
    Camera(camera_id="CAM-02", zone="ligne_mediane_tribune_principale",
           zone_type="midline", event_probability=0.45),
    Camera(camera_id="CAM-03", zone="corner_tribune_principale_droite",
           zone_type="corner", event_probability=0.30),
    Camera(camera_id="CAM-04", zone="cage_droite",
           zone_type="goal", event_probability=0.22),
    Camera(camera_id="CAM-05", zone="corner_tribune_opposee_droite",
           zone_type="corner", event_probability=0.30),
    Camera(camera_id="CAM-06", zone="ligne_mediane_tribune_opposee",
           zone_type="midline", event_probability=0.45),
    Camera(camera_id="CAM-07", zone="corner_tribune_opposee_gauche",
           zone_type="corner", event_probability=0.30),
    Camera(camera_id="CAM-08", zone="cage_gauche",
           zone_type="goal", event_probability=0.22),
]

TEAMS = ["home", "away"]

# Pondération des types d'événements selon le type de zone couverte par la caméra.
EVENT_WEIGHTS_BY_ZONE = {
    "midline": {
        "player_detection": 28, "ball_possession_change": 24, "pass": 26,
        "sprint": 12, "tackle": 6, "shot_on_target": 1, "foul": 2,
        "offside": 1, "corner": 0, "goal": 0,
    },
    "corner": {
        "player_detection": 22, "ball_possession_change": 14, "pass": 16,
        "sprint": 10, "tackle": 12, "shot_on_target": 3, "foul": 8,
        "offside": 3, "corner": 11, "goal": 1,
    },
    "goal": {
        "player_detection": 18, "ball_possession_change": 8, "pass": 8,
        "sprint": 6, "tackle": 8, "shot_on_target": 20, "foul": 6,
        "offside": 14, "corner": 6, "goal": 6,
    },
}


# ----------------------------------------------------------------------------
# 2. Réutilisable plus tard par le consommateur de déduplication en aval
# ----------------------------------------------------------------------------

# Caméras voisines dont les champs de vision se chevauchent partiellement (donc
# susceptibles de détecter le même événement réel). Fusion à faire UNIQUEMENT
# entre voisins directs, pas entre caméras opposées sur le terrain.
CAMERA_ADJACENCY = {
    "CAM-01": ["CAM-02", "CAM-08"],
    "CAM-02": ["CAM-01", "CAM-03"],
    "CAM-03": ["CAM-02", "CAM-04"],
    "CAM-04": ["CAM-03", "CAM-05"],
    "CAM-05": ["CAM-04", "CAM-06"],
    "CAM-06": ["CAM-05", "CAM-07"],
    "CAM-07": ["CAM-06", "CAM-08"],
    "CAM-08": ["CAM-07", "CAM-01"],
}

# Types d'événements pour lesquels une double détection cross-caméra est plausible.
MERGEABLE_EVENT_TYPES = {
    "ball_possession_change", "pass", "tackle", "foul",
    "offside", "shot_on_target", "goal", "corner",
}

# Fenêtre temporelle (secondes) suggérée pour la fusion en aval.
DEDUP_WINDOW_SECONDS = 2.0


# ----------------------------------------------------------------------------
# 3. Génération d'un événement unitaire (exécutée localement par chaque caméra)
# ----------------------------------------------------------------------------

def _weighted_event_type(zone_type: str) -> str:
    weights_dict = EVENT_WEIGHTS_BY_ZONE[zone_type]
    types = list(weights_dict.keys())
    weights = list(weights_dict.values())
    return random.choices(types, weights=weights, k=1)[0]


def _normalized_bbox() -> dict:
    """Bounding box normalisée [0-1] telle que renvoyée par le pipeline de détection."""
    w = round(random.uniform(0.02, 0.08), 3)
    h = round(random.uniform(0.05, 0.15), 3)
    x = round(random.uniform(0.0, 1.0 - w), 3)
    y = round(random.uniform(0.0, 1.0 - h), 3)
    return {"x": x, "y": y, "w": w, "h": h}


def generate_event(camera: Camera, match_minute: int, match_second: int) -> dict:
    event_type = _weighted_event_type(camera.zone_type)
    team = random.choice(TEAMS)

    event = {
        "event_id": str(uuid.uuid4()),
        "camera_id": camera.camera_id,
        "zone": camera.zone,
        "device": camera.device,
        "model": camera.model,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "match_time": f"{match_minute:02d}:{match_second:02d}",
        "event_type": event_type,
        "team": team,
        "player_jersey": random.randint(1, 26),
        "confidence": round(random.uniform(0.72, 0.98), 3),
        "bbox": _normalized_bbox(),
        "inference_latency_ms": round(random.uniform(35, 95), 1),
    }

    if event_type == "sprint":
        event["speed_kmh"] = round(random.uniform(18, 33), 1)
    elif event_type == "shot_on_target":
        event["distance_to_goal_m"] = round(random.uniform(5, 25), 1)
        event["shot_speed_kmh"] = round(random.uniform(60, 110), 1)
    elif event_type == "goal":
        event["distance_to_goal_m"] = round(random.uniform(3, 18), 1)
    elif event_type == "offside":
        event["margin_m"] = round(random.uniform(0.1, 1.5), 2)
    elif event_type == "tackle":
        event["outcome"] = random.choice(["won", "lost", "foul_committed"])

    return event


# ----------------------------------------------------------------------------
# 4. Stub de publication Kafka — un topic dédié PAR CAMÉRA
# ----------------------------------------------------------------------------

class KafkaPublisher:
    """
    Producteur Kafka réel, avec repli automatique en mode simulation si la
    bibliothèque kafka-python n'est pas installée ou si le broker est injoignable
    (pratique pour développer/tester sans dépendance Kafka disponible).

    Configuration via variables d'environnement — permet de basculer entre Kafka
    local (Docker, coût nul) et un cluster managé cloud (ex. Confluent Cloud) sans
    changer une ligne de code :

        KAFKA_BOOTSTRAP_SERVERS   ex. "localhost:9092" (Docker) ou
                                   "pkc-xxxxx.region.aws.confluent.cloud:9092" (Confluent Cloud)
                                   ou "5.tcp.eu.ngrok.io:16888" (Docker local via tunnel ngrok)
        KAFKA_SECURITY_PROTOCOL   "PLAINTEXT" (défaut, sans authentification) ou
                                   "SASL_SSL" (Confluent Cloud) ou
                                   "SASL_PLAINTEXT" (Docker local authentifié, ex. via tunnel ngrok)
        KAFKA_API_KEY             requis si SASL_SSL ou SASL_PLAINTEXT (nom d'utilisateur SASL)
        KAFKA_API_SECRET          requis si SASL_SSL ou SASL_PLAINTEXT (mot de passe SASL)
    """

    def __init__(self, topic: str, bootstrap_servers: str = None):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.published_count = 0
        self._producer = None
        self._connect()

    def _connect(self) -> None:
        try:
            from kafka import KafkaProducer
        except ImportError:
            print(f"[KafkaPublisher] kafka-python non installé — mode simulation "
                  f"pour le topic '{self.topic}' (pip install kafka-python pour activer)")
            return

        security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        conf = {
            "bootstrap_servers": self.bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            "security_protocol": security_protocol,
            "request_timeout_ms": 5000,
            "max_block_ms": 5000,  # évite un blocage de 60s (défaut) si le broker est injoignable
            "api_version": (2, 5, 0),  # évite la négociation auto (lente/instable sans broker joignable)
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
            conf.update({
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": os.environ.get("KAFKA_API_KEY", ""),
                "sasl_plain_password": os.environ.get("KAFKA_API_SECRET", ""),
            })

        try:
            self._producer = KafkaProducer(**conf)
            print(f"[KafkaPublisher] producteur initialisé pour '{self.bootstrap_servers}' "
                  f"({security_protocol}) — topic '{self.topic}' "
                  f"(connexion réelle vérifiée au premier envoi)")
        except Exception as exc:
            print(f"[KafkaPublisher] configuration invalide pour '{self.bootstrap_servers}' "
                  f"({exc}) — mode simulation pour le topic '{self.topic}'")
            self._producer = None

    def publish(self, event: dict) -> None:
        if self._producer is not None:
            try:
                future = self._producer.send(self.topic, value=event)
                future.get(timeout=5)  # force la levée d'exception si l'envoi échoue réellement
            except Exception as exc:
                print(f"[KafkaPublisher] échec de publication sur '{self.topic}' "
                      f"({exc}) — broker injoignable ou mal configuré")
                self._producer = None  # évite de retenter à chaque événement suivant
        self.published_count += 1
