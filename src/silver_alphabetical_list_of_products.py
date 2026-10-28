# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

source_table = "odatacatalog_dev.bronze.alphabetical_list_of_products"
target_table = "odatacatalog_dev.silver.alphabetical_list_of_products"


bronze_df = spark.read.table(source_table)

silver_df = (
    bronze_df
    .dropDuplicates(["productid", "ProductID"])
    .withColumn("updated_at", F.current_timestamp())
)

print(f"Bronze records: {bronze_df.count()}")
print(f"Records after deduplication: {silver_df.count()}")

if not spark.catalog.tableExists("odatacatalog_dev.silver.alphabetical_list_of_products"):

    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    print("Silver table created")

else:

    target = DeltaTable.forName(
        spark,
        "odatacatalog_dev.silver.alphabetical_list_of_products"
    )

    (
        target.alias("target")
        .merge(
            silver_df.alias("source"),
            """
            target.productid = source.productid
            AND target.ProductID = source.ProductID
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver table upsert completed")

silver_final_df = spark.read.table(target_table)

print(f"Final Silver records: {silver_final_df.count()}")

display(silver_final_df)