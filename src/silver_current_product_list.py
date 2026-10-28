# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

source_table = "odatacatalog_dev.bronze.current_product_lists"
target_table = "odatacatalog_dev.silver.current_product_lists"



bronze_df = spark.read.table(source_table)

silver_df = (
    bronze_df
    .dropDuplicates(["ProductID"])
    .withColumn("updated_at", F.current_timestamp())
)

print(f"Bronze records: {bronze_df.count()}")
print(f"Records after deduplication: {silver_df.count()}")

if not spark.catalog.tableExists("odatacatalog_dev.silver.current_product_lists"):

    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    print("Silver table created")

else:

    target = DeltaTable.forName(
        spark,
        "odatacatalog_dev.silver.current_product_lists"
    )

    (
        target.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.ProductID = source.ProductID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver table upsert completed")

silver_df_final = spark.read.table(target_table)

print(f"Final Silver records: {silver_df_final.count()}")

display(silver_df_final)