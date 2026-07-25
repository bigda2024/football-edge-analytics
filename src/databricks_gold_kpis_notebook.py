# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — KPIs Équipe & Heatmap (Dashboard Coach temps réel)
# MAGIC
# MAGIC Lit la table Silver dédupliquée (`match_events_silver`) et produit 2 tables Gold consommées
# MAGIC par le dashboard Lakeview :
# MAGIC
# MAGIC 1. `gold.team_kpis`     — vue équipe, par fenêtre glissante 5min/1min (tendance de match)
# MAGIC 2. `gold.zone_heatmap` — densité d'événements par zone/équipe, cumulée (upsert)
# MAGIC
# MAGIC **Contrainte Free Edition** : `trigger(availableNow=True)` partout, pas de streaming continu.
# MAGIC
# MAGIC **⚠️ Limitation actuelle — table `player_kpis` retirée temporairement** : le schéma réel de
# MAGIC `match_events_silver` ne contient pas `player_jersey`, `speed_kmh`, `distance_to_goal_m`,
# MAGIC `shot_speed_kmh`, `margin_m` ni `outcome` — ces champs existent dans Bronze (issus du
# MAGIC simulateur) mais n'ont pas été propagés par le notebook de déduplication Silver, qui ne
# MAGIC conserve actuellement que les champs communs à tous les types d'événements plus les champs
# MAGIC de dédup (`source_cameras`, `camera_count`, `dedup_status`, `merged_with_event_id`). Sans
# MAGIC `player_jersey`, aucune vue par joueur n'est possible. À corriger côté notebook Silver avant
# MAGIC de réintroduire `player_kpis` (cf. section limites méthodologiques — Unit A, thèse).
# MAGIC
# MAGIC **Choix d'architecture à noter (soutenance)** : le calcul de `possession_pct` nécessite une
# MAGIC fonction window non-temporelle (`Window.partitionBy` sur la fenêtre de temps), ce qui est
# MAGIC **interdit directement sur un DataFrame streaming** dans Spark Structured Streaming — même
# MAGIC en mode `availableNow`. On passe donc par `foreachBatch`, qui transforme chaque micro-batch
# MAGIC en DataFrame batch classique où ces fonctions redeviennent utilisables. C'est la même famille
# MAGIC de contrainte que l'interdiction des UDF Python dans les clauses `LEFT OUTER JOIN ON`, et que
# MAGIC l'absence de `.rdd` sur Spark Connect (serverless) — trois manifestations du même principe :
# MAGIC le mode serverless de Databricks Free Edition restreint l'accès aux API Spark bas niveau.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Catalog (dev/uat/prod)")
catalog = dbutils.widgets.get("catalog")

SILVER_TABLE = f"{catalog}.silver.match_events_silver"
GOLD_TEAM_KPIS = f"{catalog}.gold.team_kpis"
GOLD_ZONE_HEATMAP = f"{catalog}.gold.zone_heatmap"

CHECKPOINT_TEAM = f"/Volumes/{catalog}/ops/checkpoints/gold_team_kpis"
CHECKPOINT_ZONE = f"/Volumes/{catalog}/ops/checkpoints/gold_zone_heatmap"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_df = spark.readStream.table(SILVER_TABLE)

action_events = silver_df.filter(F.col("event_type") != "player_detection")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TEAM_KPIS} (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    team STRING,
    total_events LONG,
    possession_pct DOUBLE,
    sprint_count LONG,
    shots_on_target LONG,
    goals LONG,
    tackles LONG,
    offsides LONG,
    avg_detection_confidence DOUBLE
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
            F.sum(F.when(F.col("event_type") == "sprint", 1).otherwise(0)).alias("sprint_count"),
            F.sum(F.when(F.col("event_type") == "shot_on_target", 1).otherwise(0)).alias("shots_on_target"),
            F.sum(F.when(F.col("event_type") == "goal", 1).otherwise(0)).alias("goals"),
            F.sum(F.when(F.col("event_type") == "tackle", 1).otherwise(0)).alias("tackles"),
            F.sum(F.when(F.col("event_type") == "offside", 1).otherwise(0)).alias("offsides"),
            F.avg("confidence").alias("avg_detection_confidence"),
        )
    )

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
        "sprint_count",
        "shots_on_target",
        "goals",
        "tackles",
        "offsides",
        F.round("avg_detection_confidence", 3).alias("avg_detection_confidence"),
    )

    result.write.format("delta").mode("append").saveAsTable(GOLD_TEAM_KPIS)


team_query = (
    action_events.writeStream.foreachBatch(compute_team_kpis_batch)
    .option("checkpointLocation", CHECKPOINT_TEAM)
    .trigger(availableNow=True)
    .start()
)

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

team_query.awaitTermination()
zone_query.awaitTermination()

print("Gold layer terminé :")
print(f"  - {GOLD_TEAM_KPIS}")
print(f"  - {GOLD_ZONE_HEATMAP}")
print("  - gold.player_kpis : NON GENERE (player_jersey absent de Silver, cf. note en tete de notebook)")