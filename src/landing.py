# Databricks notebook source
# DBTITLE 1,Job parameter widget
dbutils.widgets.text("entity", "ALL")

entity_param = dbutils.widgets.get("entity").strip()

print(f"Job parameter: {entity_param}")

# COMMAND ----------

# DBTITLE 1,Load config and select entities
import requests
import json
from datetime import datetime

config_path = "/Volumes/odatacatalog_dev/odata-metadata/odata-metadata/config.json"

config_text = dbutils.fs.head(config_path)
config = json.loads(config_text)

base_url = config["base_url"]
all_entities = config["entities"]

landing_root = "/Volumes/odatacatalog_dev/landing/landing-volume"
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


for entity in entities_to_process:

    print(f"\nProcessing entity: {entity}")

    # Entity folder path
    entity_path = f"{landing_root}/{entity}"

    # Create entity folder even if API returns no records
    dbutils.fs.mkdirs(entity_path)

    next_url = f"{base_url}/{entity}"
    all_records = []
    page_number = 1

    while next_url:

        print(f"Fetching page {page_number}: {next_url}")

        response = requests.get(next_url, timeout=60)

        if response.status_code != 200:
            print(f"Failed: {entity} - HTTP {response.status_code}")
            break

        data = response.json()

        records = data.get("value", [])

        print(f"Page {page_number}: {len(records)} records")

        all_records.extend(records)

        next_link = data.get("@odata.nextLink")

        if next_link:
            from urllib.parse import urljoin
            next_url = urljoin(base_url + "/", next_link)
        else:
            next_url = None

        page_number += 1

    # If no records were found
    if not all_records:

        print(f"No records found for {entity}")

        # Create an empty JSON file so Bronze can read the entity
        empty_data = {"value": []}

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = f"{entity_path}/{entity}_{timestamp}.json"

        dbutils.fs.put(
            file_path,
            json.dumps(empty_data, indent=2),
            overwrite=False
        )

        print(f"Empty Landing file created: {file_path}")
        continue

   
    print(f"Total records received for {entity}: {len(all_records)}")

    landing_data = {
        "value": all_records
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    file_path = f"{entity_path}/{entity}_{timestamp}.json"

    dbutils.fs.put(
        file_path,
        json.dumps(landing_data, indent=2),
        overwrite=False
    )

    print(f"Landing file created: {file_path}")

# COMMAND ----------

# DBTITLE 1,Test API request with skiptoken
url = f"{base_url}/Customer_and_Suppliers_by_Cities?$skiptoken='Sp%C3%A9cialit%C3%A9s%20du%20monde','Customers'"

response = requests.get(url, timeout=60)
data = response.json()

print("HTTP Status:", response.status_code)
print("Records received:", len(data.get("value", [])))
print("Response keys:", list(data.keys()))
print("Next Link:", data.get("@odata.nextLink"))

# COMMAND ----------

all_records = []
page_number = 1

while next_url:
    print(f"Fetching page {page_number} for ")

    response = requests.get(next_url, timeout=60)
    response.raise_for_status()

    data = response.json()

    records = data.get("value", [])

    print(f"Page {page_number}: {len(records)} records")

    all_records.extend(records)

    next_link = data.get("@odata.nextLink")

    if next_link:
        next_url = urljoin(base_url + "/", next_link)
    else:
        next_url = None

    page_number += 1

# FINAL ENTITY COUNT
print(f"Total records received for {entity}: {len(all_records)}")