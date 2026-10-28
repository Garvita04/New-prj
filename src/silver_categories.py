# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

source_table = "odatacatalog_dev.bronze.categories"
target_table = "odatacatalog_dev.silver.categories"


bronze_df = spark.read.table(source_table)

silver_df = (
    bronze_df
    .dropDuplicates(["CategoryID"])
    .withColumn("updated_at", F.current_timestamp())
)

print(f"Bronze records: {bronze_df.count()}")
print(f"Records after deduplication: {silver_df.count()}")

if not spark.catalog.tableExists("odatacatalog_dev.silver.categories"):

    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    print("Silver table created")

else:

    target = DeltaTable.forName(
        spark,
        "odatacatalog_dev.silver.categories"
    )

    (
        target.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.CategoryID = source.CategoryID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver table upsert completed")

silver_df_final = spark.read.table(target_table)

print(f"Final Silver records: {silver_df_final.count()}")

display(silver_df_final)