# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — KPIs Équipe, Joueur & Heatmap (Dashboard Coach temps réel)
# MAGIC
# MAGIC Lit la table Silver dédupliquée (`match_events`) et produit 3 tables Gold consommées
# MAGIC par le dashboard Lakeview :
# MAGIC
# MAGIC 1. `gold.team_kpis`     — vue équipe, par fenêtre glissante 5min/1min (tendance de match)
# MAGIC 2. `gold.player_kpis`  — vue joueur, cumulée sur tout le match (upsert)
# MAGIC 3. `gold.zone_heatmap` — densité d'événements par zone/équipe, cumulée (upsert)
# MAGIC
# MAGIC **Contrainte Free Edition** : `trigger(availableNow=True)` partout, pas de streaming continu.
# MAGIC
# MAGIC **Choix d'architecture à noter (soutenance)** : le calcul de `possession_pct` nécessite une
# MAGIC fonction window non-temporelle (`Window.partitionBy` sur la fenêtre de temps), ce qui est
# MAGIC **interdit directement sur un DataFrame streaming** dans Spark Structured Streaming — même
# MAGIC en mode `availableNow`. On passe donc par `foreachBatch`, qui transforme chaque micro-batch
# MAGIC en DataFrame batch classique où ces fonctions redeviennent utilisables. C'est la même famille
# MAGIC de contrainte que l'interdiction des UDF Python dans les clauses `LEFT OUTER JOIN ON` déjà
# MAGIC rencontrée dans le notebook de déduplication Silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Catalog (dev/uat/prod)")
catalog = dbutils.widgets.get("catalog")

SILVER_TABLE = f"{catalog}.silver.match_events_silver"
GOLD_TEAM_KPIS = f"{catalog}.gold.team_kpis"
GOLD_PLAYER_KPIS = f"{catalog}.gold.player_kpis"
GOLD_ZONE_HEATMAP = f"{catalog}.gold.zone_heatmap"

CHECKPOINT_TEAM = f"/Volumes/{catalog}/ops/checkpoints/gold_team_kpis"
CHECKPOINT_PLAYER = f"/Volumes/{catalog}/ops/checkpoints/gold_player_kpis"
CHECKPOINT_ZONE = f"/Volumes/{catalog}/ops/checkpoints/gold_zone_heatmap"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_df = spark.readStream.table(SILVER_TABLE)

# Les player_detection sont du bruit de fond (présence, pas d'action) — exclus des KPIs
# de jeu, cohérent avec l'exclusion déjà faite pour la dédup cross-caméra.
action_events = silver_df.filter(F.col("event_type") != "player_detection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Création des tables Gold (schéma explicite, si absentes)
# MAGIC
# MAGIC Nécessaire avant le premier MERGE : `DeltaTable.forName` échoue si la table n'existe pas.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TEAM_KPIS} (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    team STRING,
    total_events LONG,
    possession_pct DOUBLE,
    estimated_distance_km DOUBLE,
    sprint_count LONG,
    shots_on_target LONG,
    goals LONG,
    tackles LONG,
    tackles_won LONG,
    offsides LONG,
    avg_detection_confidence DOUBLE
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_PLAYER_KPIS} (
    team STRING,
    player_jersey INT,
    sprint_count LONG,
    max_speed_kmh DOUBLE,
    avg_speed_kmh DOUBLE,
    estimated_distance_km DOUBLE,
    tackles LONG,
    tackles_won LONG,
    shots_on_target LONG,
    goals LONG,
    last_updated TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_ZONE_HEATMAP} (
    team STRING,
    zone STRING,
    event_count LONG,
    avg_confidence DOUBLE,
    last_updated TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Team KPIs — fenêtre glissante 5min/1min
# MAGIC
# MAGIC - **`possession_pct`** : proxy basé sur la part du volume d'événements d'action par équipe
# MAGIC   dans la fenêtre — **ce n'est pas une vraie mesure de possession** (nécessiterait un tracking
# MAGIC   continu du ballon). À documenter explicitement comme limite méthodologique dans la thèse
# MAGIC   (section limites, unité d'analyse "performance technique du système").
# MAGIC - **`estimated_distance_km`** : dérivée des événements `sprint` uniquement (vitesse instantanée
# MAGIC   × durée forfaitaire de 3s par sprint détecté) — proxy également, pas un tracking GPS/UWB.

# COMMAND ----------

def compute_team_kpis_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    windowed = (
        batch_df
        .groupBy(
            F.window("timestamp_utc", "5 minutes", "1 minute").alias("time_window"),
            "team",
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "sprint", F.col("speed_kmh") * (3 / 3600.0)).otherwise(0.0)).alias(
                "estimated_distance_km"
            ),
            F.sum(F.when(F.col("event_type") == "sprint", 1).otherwise(0)).alias("sprint_count"),
            F.sum(F.when(F.col("event_type") == "shot_on_target", 1).otherwise(0)).alias("shots_on_target"),
            F.sum(F.when(F.col("event_type") == "goal", 1).otherwise(0)).alias("goals"),
            F.sum(F.when(F.col("event_type") == "tackle", 1).otherwise(0)).alias("tackles"),
            F.sum(
                F.when((F.col("event_type") == "tackle") & (F.col("outcome") == "won"), 1).otherwise(0)
            ).alias("tackles_won"),
            F.sum(F.when(F.col("event_type") == "offside", 1).otherwise(0)).alias("offsides"),
            F.avg("confidence").alias("avg_detection_confidence"),
        )
    )

    # Fonction window non-temporelle : possible ici car batch_df/windowed sont des DataFrames
    # batch classiques à l'intérieur de foreachBatch (pas des streams).
    with_possession = windowed.withColumn(
        "possession_pct",
        F.round(
            F.col("total_events") * 100.0 / F.sum("total_events").over(Window.partitionBy("time_window")),
            1,
        ),
    )

    result = with_possession.select(
        F.col("time_window.start").alias("window_start"),
        F.col("time_window.end").alias("window_end"),
        "team",
        "total_events",
        "possession_pct",
        F.round("estimated_distance_km", 3).alias("estimated_distance_km"),
        "sprint_count",
        "shots_on_target",
        "goals",
        "tackles",
        "tackles_won",
        "offsides",
        F.round("avg_detection_confidence", 3).alias("avg_detection_confidence"),
    )

    # Append simple : chaque micro-batch ajoute ses fenêtres. Le dashboard Lakeview agrège
    # ensuite côté requête (dernière fenêtre par équipe = tendance courante).
    result.write.format("delta").mode("append").saveAsTable(GOLD_TEAM_KPIS)


team_query = (
    action_events.writeStream.foreachBatch(compute_team_kpis_batch)
    .option("checkpointLocation", CHECKPOINT_TEAM)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Player KPIs — cumulés sur tout le match (MERGE upsert)
# MAGIC
# MAGIC Identifiant joueur = `(team, player_jersey)` — pas de `player_id` unique dans le schéma
# MAGIC de simulation actuel, à faire évoluer si un système de tracking d'identité est ajouté côté edge.

# COMMAND ----------

def compute_player_kpis_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_agg = batch_df.groupBy("team", "player_jersey").agg(
        F.sum(F.when(F.col("event_type") == "sprint", 1).otherwise(0)).alias("sprint_count"),
        F.max(F.when(F.col("event_type") == "sprint", F.col("speed_kmh"))).alias("max_speed_kmh"),
        F.avg(F.when(F.col("event_type") == "sprint", F.col("speed_kmh"))).alias("avg_speed_kmh"),
        F.sum(F.when(F.col("event_type") == "sprint", F.col("speed_kmh") * (3 / 3600.0)).otherwise(0.0)).alias(
            "estimated_distance_km"
        ),
        F.sum(F.when(F.col("event_type") == "tackle", 1).otherwise(0)).alias("tackles"),
        F.sum(
            F.when((F.col("event_type") == "tackle") & (F.col("outcome") == "won"), 1).otherwise(0)
        ).alias("tackles_won"),
        F.sum(F.when(F.col("event_type") == "shot_on_target", 1).otherwise(0)).alias("shots_on_target"),
        F.sum(F.when(F.col("event_type") == "goal", 1).otherwise(0)).alias("goals"),
    ).withColumn("last_updated", F.current_timestamp())

    gold_table = DeltaTable.forName(spark, GOLD_PLAYER_KPIS)

    (
        gold_table.alias("t")
        .merge(
            batch_agg.alias("s"),
            "t.team = s.team AND t.player_jersey = s.player_jersey",
        )
        .whenMatchedUpdate(
            set={
                "sprint_count": "t.sprint_count + s.sprint_count",
                "max_speed_kmh": "greatest(t.max_speed_kmh, s.max_speed_kmh)",
                # moyenne pondérée par le nb de sprints déjà vus vs nouveaux
                "avg_speed_kmh": (
                    "(t.avg_speed_kmh * t.sprint_count + s.avg_speed_kmh * s.sprint_count) "
                    "/ nullif(t.sprint_count + s.sprint_count, 0)"
                ),
                "estimated_distance_km": "t.estimated_distance_km + s.estimated_distance_km",
                "tackles": "t.tackles + s.tackles",
                "tackles_won": "t.tackles_won + s.tackles_won",
                "shots_on_target": "t.shots_on_target + s.shots_on_target",
                "goals": "t.goals + s.goals",
                "last_updated": "s.last_updated",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


player_query = (
    action_events.writeStream.foreachBatch(compute_player_kpis_batch)
    .option("checkpointLocation", CHECKPOINT_PLAYER)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Zone Heatmap — densité d'événements par zone/équipe (proxy)
# MAGIC
# MAGIC Faute de coordonnées pitch réelles (seule la `bbox` normalisée **par caméra** est disponible,
# MAGIC pas de calibration homographique caméra → terrain à ce stade), la heatmap est construite sur
# MAGIC la `zone` de la caméra source plutôt que sur des coordonnées (x, y) continues. À faire évoluer
# MAGIC vers une vraie heatmap continue une fois la calibration caméra→terrain ajoutée (cf. section
# MAGIC "à venir" côté hardware réel).

# COMMAND ----------

def compute_zone_heatmap_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_agg = batch_df.groupBy("team", "zone").agg(
        F.count("*").alias("event_count"),
        F.avg("confidence").alias("avg_confidence"),
    ).withColumn("last_updated", F.current_timestamp())

    gold_table = DeltaTable.forName(spark, GOLD_ZONE_HEATMAP)

    (
        gold_table.alias("t")
        .merge(batch_agg.alias("s"), "t.team = s.team AND t.zone = s.zone")
        .whenMatchedUpdate(
            set={
                "event_count": "t.event_count + s.event_count",
                "avg_confidence": (
                    "(t.avg_confidence * t.event_count + s.avg_confidence * s.event_count) "
                    "/ nullif(t.event_count + s.event_count, 0)"
                ),
                "last_updated": "s.last_updated",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


zone_query = (
    action_events.writeStream.foreachBatch(compute_zone_heatmap_batch)
    .option("checkpointLocation", CHECKPOINT_ZONE)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Attente de fin des 3 jobs (mode availableNow = traitement fini puis arrêt)

# COMMAND ----------

team_query.awaitTermination()
player_query.awaitTermination()
zone_query.awaitTermination()

print("Gold layer terminé :")
print(f"  - {GOLD_TEAM_KPIS}")
print(f"  - {GOLD_PLAYER_KPIS}")
print(f"  - {GOLD_ZONE_HEATMAP}")