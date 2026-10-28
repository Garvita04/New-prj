# Databricks notebook source
# DBTITLE 1,Discover OData entities
import requests
import json
import xml.etree.ElementTree as ET

# OData metadata URL
metadata_url = "https://services.odata.org/V4/Northwind/Northwind.svc/$metadata"

# Call API
response = requests.get(metadata_url)

if response.status_code != 200:
    raise Exception(f"API failed: {response.status_code}")

# Parse metadata XML
root = ET.fromstring(response.text)

# OData namespace
namespace = {
    "edm": "http://docs.oasis-open.org/odata/ns/edm"
}

# Discover entity sets dynamically
entity_sets = []

for entity_set in root.findall(".//edm:EntitySet", namespace):
    name = entity_set.get("Name")
    entity_sets.append(name)

print(f"Found {len(entity_sets)} entities")

for entity in entity_sets:
    print(entity)

# Create config dynamically
config = {
    "base_url": "https://services.odata.org/V4/Northwind/Northwind.svc",
    "entities": entity_sets
}

# Convert to JSON
config_json = json.dumps(config, indent=4)

print(config_json)

# COMMAND ----------

# DBTITLE 1,Write config.json to volume
config_path = "/Volumes/odatacatalog_dev/odata-metadata/odata-metadata/config.json"

dbutils.fs.put(
    config_path,
    config_json,
    overwrite=True
)

print("Config created successfully")

# COMMAND ----------

