# Databricks notebook source
# DBTITLE 1,Job parameter widget
dbutils.widgets.text("entity", "ALL")

entity_param = dbutils.widgets.get("entity").strip()

print(f"Job parameter: {entity_param}")

# COMMAND ----------

import json



config_path = "/Volumes/odatacatalog_dev/odata-metadata/odata-metadata/config.json"

config_text = dbutils.fs.head(config_path)
config = json.loads(config_text)

all_entities = config["entities"]

if entity_param.upper() == "ALL":
    entities_to_process = all_entities
else:
    matching_entities = [
        entity
        for entity in all_entities
        if entity.lower() == entity_param.lower()
    ]

    if not matching_entities:
        raise Exception(
            f"Entity '{entity_param}' was not found in config.json"
        )

    entities_to_process = matching_entities

print("Entities to process:")

for entity in entities_to_process:
    print(f" - {entity}")



landing_root = "/Volumes/odatacatalog_dev/landing/landing-volume"

for entity in entities_to_process:

    print(f"\n{'=' * 50}")
    print(f"Processing entity: {entity}")
    print(f"{'=' * 50}")

    landing_path = f"{landing_root}/{entity}"

    table_name = f"odatacatalog_dev.bronze.{entity}"

    checkpoint_path = (
        f"/Volumes/odatacatalog_dev/landing/landing-volume/checkpoints/{entity}"
    )

    schema_path = (
        f"/Volumes/odatacatalog_dev/landing/landing-volume/schemas/{entity}"
    )

    try:
        files = dbutils.fs.ls(landing_path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "No such file or directory" in str(e):
            print(f"Skipping {entity}: Landing folder not found")
            continue
        else:
            raise

    if len(files) == 0:
        print(f"Skipping {entity}: Landing folder is empty")
        continue

    try:
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", schema_path)
            .load(landing_path)
        )

        if "value" not in df.columns:
            print(f"Skipping {entity}: value column not found")
            continue

        records_df = (
            df
            .selectExpr("explode(value) as record")
            .select("record.*")
        )

        query = (
            records_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("mergeSchema", "true")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(table_name)
        )

        query.awaitTermination()

    except Exception as e:
        if "CF_FAILED_TO_INFER_SCHEMA" in str(e):
            print(f"Skipping {entity}: Cannot infer schema from files (possibly empty data)")
            continue
        else:
            raise

    bronze_df = spark.read.table(table_name)

    print(f"Bronze table: {table_name}")
    print(f"Bronze records: {bronze_df.count()}")

   