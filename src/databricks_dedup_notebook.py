# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Kafka + fusion cross-caméra — Embedded Case
# MAGIC
# MAGIC Lit les 8 topics Kafka (un par caméra, `match-events.cam-01` à `cam-08`) via
# MAGIC Spark Structured Streaming, applique la logique de fusion cross-caméra (mise
# MAGIC de côté au niveau des scripts caméra pour être traitée ici, en aval), et écrit
# MAGIC le résultat dans des tables Delta selon une architecture medallion simplifiée :
# MAGIC
# MAGIC - **bronze** : événements bruts, un par détection caméra, tels que reçus de Kafka
# MAGIC - **silver** : événements après fusion cross-caméra, prêts pour le dashboard coach
# MAGIC
# MAGIC ⚠️ Databricks Free Edition tourne en serverless : seul `trigger(availableNow=True)`
# MAGIC est supporté (pas de streaming continu `ProcessingTime`). Chaque exécution de ce
# MAGIC notebook traite tout ce qui est disponible dans Kafka, puis s'arrête — à relancer
# MAGIC périodiquement (manuellement ou via un Job planifié) pour ingérer les lots suivants.

# COMMAND ----------

# MAGIC %md ## 1. Paramètres de connexion et d'environnement
# MAGIC
# MAGIC Utilise des widgets Databricks pour éviter de modifier le code à chaque session
# MAGIC (adresse ngrok qui change, etc.) et pour permettre de rejouer exactement le même
# MAGIC notebook en dev, uat ou prod sans dupliquer le code — seul le paramètre
# MAGIC `environment` change, ce qui détermine le catalogue Unity Catalog ciblé
# MAGIC (`<environment>.bronze.*`, `<environment>.silver.*`) et le chemin de checkpoint.
# MAGIC Pour un usage au-delà de ce pilote, remplace les identifiants Kafka en dur par
# MAGIC des secrets Databricks (`dbutils.secrets.get`).

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "uat", "prod"])
dbutils.widgets.text("kafka_bootstrap_servers", "4.tcp.eu.ngrok.io:11270")
dbutils.widgets.text("kafka_username", "admin")

ENVIRONMENT = dbutils.widgets.get("environment")
KAFKA_BOOTSTRAP_SERVERS = dbutils.widgets.get("kafka_bootstrap_servers")
KAFKA_USERNAME = dbutils.widgets.get("kafka_username")
# Le mot de passe est lu directement via l'utilitaire Secrets, PAS via un widget/
# base_parameter : la syntaxe "{{secrets/scope/key}}" n'est pas résolue dans les
# base_parameters d'une tâche notebook (uniquement dans certains contextes cluster).
KAFKA_PASSWORD = dbutils.secrets.get(scope="football_pipeline", key="kafka_password")

# Convention : <environment>.<couche>.<table>, ex. dev.bronze.match_events_bronze
# Si ton instance Free Edition n'autorise qu'un seul catalogue, remplace par des
# schémas préfixés dans un catalogue unique (ex. main.dev_bronze, main.dev_silver).
BRONZE_TABLE = f"{ENVIRONMENT}.bronze.match_events_bronze"
SILVER_TABLE = f"{ENVIRONMENT}.silver.match_events_silver"
# Chemin de volume Unity Catalog complet : /Volumes/<catalogue>/<schéma>/<volume>/...
# Le schéma "ops" et le volume "checkpoints" doivent exister au préalable
# (voir setup_unity_catalog.sql) — un simple répertoire ne suffit pas en UC.
CHECKPOINT_BASE = f"/Volumes/{ENVIRONMENT}/ops/checkpoints/match_events"

CAMERA_IDS = [f"CAM-0{i}" for i in range(1, 9)]
TOPICS = [f"match-events.{cam.lower()}" for cam in CAMERA_IDS]

# Fenêtre de fusion cross-caméra (secondes) — doit correspondre à ce qui était
# prévu côté simulateur (DEDUP_WINDOW_SECONDS dans camera_config.py)
DEDUP_WINDOW_SECONDS = 2

# Graphe d'adjacence de la boucle périmétrique à 8 caméras (repris de camera_config.py)
ADJACENCY_PAIRS = {
    frozenset({"CAM-01", "CAM-02"}), frozenset({"CAM-02", "CAM-03"}),
    frozenset({"CAM-03", "CAM-04"}), frozenset({"CAM-04", "CAM-05"}),
    frozenset({"CAM-05", "CAM-06"}), frozenset({"CAM-06", "CAM-07"}),
    frozenset({"CAM-07", "CAM-08"}), frozenset({"CAM-08", "CAM-01"}),
}

# Types d'événements pour lesquels une fusion cross-caméra est pertinente
MERGEABLE_EVENT_TYPES = {
    "ball_possession_change", "pass", "tackle", "foul",
    "offside", "shot_on_target", "goal", "corner",
}

print(f"Environnement    : {ENVIRONMENT}")
print(f"Table bronze     : {BRONZE_TABLE}")
print(f"Table silver     : {SILVER_TABLE}")
print(f"Checkpoint base  : {CHECKPOINT_BASE}")
print(f"Kafka bootstrap  : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topics souscrits : {TOPICS}")

# COMMAND ----------

# MAGIC %md ## 2. Lecture du flux Kafka (8 topics, un abonnement combiné)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

JAAS_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", JAAS_CONFIG)
    # Timeouts étendus : le tunnel ngrok ajoute une latence de relais qui peut
    # dépasser les délais par défaut du client Kafka lors de la reconnexion à
    # l'adresse "officielle" (KAFKA_ADVERTISED_LISTENERS) après le bootstrap initial.
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.default.api.timeout.ms", "60000")
    .option("kafka.connections.max.idle.ms", "60000")
    .option("subscribe", ",".join(TOPICS))
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# COMMAND ----------

# MAGIC %md ## 3. Parsing du JSON — schéma aligné sur generate_event() (camera_config.py)

# COMMAND ----------

bbox_schema = StructType([
    StructField("x", DoubleType()),
    StructField("y", DoubleType()),
    StructField("w", DoubleType()),
    StructField("h", DoubleType()),
])

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("camera_id", StringType()),
    StructField("zone", StringType()),
    StructField("device", StringType()),
    StructField("model", StringType()),
    StructField("timestamp_utc", StringType()),
    StructField("match_time", StringType()),
    StructField("event_type", StringType()),
    StructField("team", StringType()),
    StructField("player_jersey", IntegerType()),
    StructField("confidence", DoubleType()),
    StructField("bbox", bbox_schema),
    StructField("inference_latency_ms", DoubleType()),
    # Champs optionnels spécifiques à certains types d'événements
    StructField("speed_kmh", DoubleType()),
    StructField("distance_to_goal_m", DoubleType()),
    StructField("shot_speed_kmh", DoubleType()),
    StructField("margin_m", DoubleType()),
    StructField("outcome", StringType()),
])

parsed = (
    raw_stream
    .select(F.col("topic"), F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
    .select("topic", "e.*")
    .withColumn("event_ts", F.to_timestamp("timestamp_utc"))
)

# COMMAND ----------

# MAGIC %md ## 4. Bronze — événements bruts, un par détection caméra (sans fusion)

# COMMAND ----------

bronze_query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)
try:
    bronze_query.awaitTermination()
except Exception as exc:
    # Le trigger availableNow=True vérifie une dernière fois s'il reste des offsets
    # après avoir traité tous les micro-batchs disponibles. Sur un tunnel ngrok
    # gratuit, cette vérification finale peut échouer par timeout MÊME QUAND toutes
    # les données ont déjà été committées avec succès (chaque micro-batch est validé
    # indépendamment). On capture cette exception pour ne pas interrompre le reste
    # du notebook (section silver) à cause d'un simple raté de fin de requête.
    print(f"Avertissement : la requête bronze s'est terminée avec une exception "
          f"({exc}). Les données déjà committées restent intactes — voir le compte "
          f"ci-dessous.")
print(f"Bronze ({BRONZE_TABLE}) : {spark.table(BRONZE_TABLE).count()} lignes au total")

# COMMAND ----------

# MAGIC %md ## 5. Silver — fusion cross-caméra
# MAGIC
# MAGIC Auto-jointure du flux sur lui-même (fenêtre temporelle + caméras voisines) pour
# MAGIC détecter les événements vus par deux caméras adjacentes et les fusionner en un
# MAGIC seul enregistrement canonique. Repose sur un *watermark* pour borner l'attente
# MAGIC avant de considérer un événement comme définitivement "sans confirmation".
# MAGIC
# MAGIC ⚠️ Simplification assumée (comme dans le simulateur) : la fusion est **pairwise**
# MAGIC (deux caméras à la fois). Un événement vu par 3 caméras adjacentes ne serait
# MAGIC fusionné qu'avec l'une d'entre elles ici — limite acceptable pour ce pilote,
# MAGIC à mentionner comme piste d'amélioration dans la discussion de l'EC.

# COMMAND ----------

# Spark interdit un UDF Python dans la condition ON d'un LEFT OUTER JOIN
# ([UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE]). On construit donc la condition
# d'adjacence avec des expressions Spark natives (comparaisons de colonnes), en
# dérivant la liste des paires ordonnées (a < b) directement de CAMERA_ADJACENCY.
_ordered_adjacent_pairs = sorted({tuple(sorted(pair)) for pair in ADJACENCY_PAIRS})

adjacency_condition = F.lit(False)
for cam_a, cam_b in _ordered_adjacent_pairs:
    adjacency_condition = adjacency_condition | (
        (F.col("a.camera_id") == cam_a) & (F.col("b.camera_id") == cam_b)
    )

mergeable = parsed.filter(F.col("event_type").isin(list(MERGEABLE_EVENT_TYPES)))
not_mergeable = (
    parsed.filter(~F.col("event_type").isin(list(MERGEABLE_EVENT_TYPES)))
    .withColumn("source_cameras", F.array(F.col("camera_id")))
    .withColumn("camera_count", F.lit(1))
    .withColumn("dedup_status", F.lit("not_applicable"))
    .withColumn("merged_with_event_id", F.lit(None).cast("string"))
)

a = mergeable.withWatermark("event_ts", "10 seconds").alias("a")
b = mergeable.withWatermark("event_ts", "10 seconds").alias("b")

join_condition = (
    (F.col("a.event_type") == F.col("b.event_type"))
    & (F.col("a.team") == F.col("b.team"))
    & (F.col("a.camera_id") < F.col("b.camera_id"))  # évite les paires en double + auto-jointure
    & adjacency_condition
    & (F.col("b.event_ts") >= F.col("a.event_ts"))
    & (F.col("b.event_ts") <= F.col("a.event_ts") + F.expr(f"INTERVAL {DEDUP_WINDOW_SECONDS} SECONDS"))
)

# LEFT OUTER : garde tous les événements de 'a', avec les infos de 'b' si un voisin
# a confirmé dans la fenêtre (NULL sinon, après expiration du watermark)
joined = a.join(b, join_condition, "left_outer")

merged_or_unique = joined.select(
    F.col("a.event_id").alias("event_id"),
    F.col("a.event_type").alias("event_type"),
    F.col("a.team").alias("team"),
    F.col("a.match_time").alias("match_time"),
    F.col("a.timestamp_utc").alias("timestamp_utc"),
    F.col("a.camera_id").alias("camera_id"),
    F.col("a.zone").alias("zone"),
    F.when(F.col("b.event_id").isNotNull(), F.greatest(F.col("a.confidence"), F.col("b.confidence")))
     .otherwise(F.col("a.confidence")).alias("confidence"),
    F.when(F.col("b.event_id").isNotNull(),
           F.array_sort(F.array(F.col("a.camera_id"), F.col("b.camera_id"))))
     .otherwise(F.array(F.col("a.camera_id"))).alias("source_cameras"),
    F.when(F.col("b.event_id").isNotNull(), F.lit(2)).otherwise(F.lit(1)).alias("camera_count"),
    F.when(F.col("b.event_id").isNotNull(), F.lit("merged")).otherwise(F.lit("unique")).alias("dedup_status"),
    F.col("b.event_id").alias("merged_with_event_id"),
)

silver_stream = merged_or_unique.unionByName(
    not_mergeable.select(
        "event_id", "event_type", "team", "match_time", "timestamp_utc",
        "camera_id", "zone", "confidence", "source_cameras", "camera_count",
        "dedup_status", "merged_with_event_id",
    )
)

silver_query = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver")
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)
try:
    silver_query.awaitTermination()
except Exception as exc:
    print(f"Avertissement : la requête silver s'est terminée avec une exception "
          f"({exc}). Les données déjà committées restent intactes — voir le compte "
          f"ci-dessous.")
print(f"Silver ({SILVER_TABLE}) : {spark.table(SILVER_TABLE).count()} lignes au total")

# COMMAND ----------

# MAGIC %md ## 6. Vérification — vue d'ensemble pour le dashboard coach

# COMMAND ----------

display(
    spark.table(SILVER_TABLE)
    .groupBy("event_type", "dedup_status")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Taux de réduction grâce à la fusion cross-caméra

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = spark.table(SILVER_TABLE).count()
reduction_pct = 100 * (1 - silver_count / bronze_count) if bronze_count else 0

print(f"Environnement                     : {ENVIRONMENT}")
print(f"Détections brutes ({BRONZE_TABLE}) : {bronze_count}")
print(f"Événements après fusion ({SILVER_TABLE}) : {silver_count}")
print(f"Réduction du volume               : {reduction_pct:.1f}%")