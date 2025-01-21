# Databricks notebook source
# MAGIC %md
# MAGIC # Import libs

# COMMAND ----------

import requests
from pyspark.sql.types import (MapType, StringType, IntegerType, LongType, TimestampType, 
                               DateType, DecimalType, BinaryType, StructType, StructField)
from pyspark.sql.functions import (collect_set, concat, lit, when, col, explode, broadcast, 
                                   collect_list, map_entries, udf, map_keys, from_unixtime)
from pyspark.sql.utils import AnalysisException
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter widgets

# COMMAND ----------

dbutils.widgets.text("SOURCE_CATALOG", "system")
dbutils.widgets.text("TARGET_CATALOG", "cog_land_system")
dbutils.widgets.text("INFO_SCHEMA_TARGET", "cog_information_schema")
dbutils.widgets.text("EXCLUDED_SCHEMAS", "ai")
dbutils.widgets.text("EXCLUDED_TABLES", "_sqldf,__internal_logging")

SOURCE_CATALOG = dbutils.widgets.get("SOURCE_CATALOG")
TARGET_CATALOG = dbutils.widgets.get("TARGET_CATALOG")
INFO_SCHEMA_TARGET = dbutils.widgets.get("INFO_SCHEMA_TARGET")
EXCLUDED_SCHEMAS = set(dbutils.widgets.get("EXCLUDED_SCHEMAS").split(","))
EXCLUDED_TABLES = set(dbutils.widgets.get("EXCLUDED_TABLES").split(","))

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Overview

# COMMAND ----------

# Load the data from the table
df = spark.table(f"{SOURCE_CATALOG}.access.audit")
df_billing_usage = spark.table(f"{SOURCE_CATALOG}.billing.usage")

# Get distinct values
distinct_account_ids = df.select("account_id").distinct()
distinct_clouds = df_billing_usage.select("cloud").distinct()

# Collect the distinct account_ids and cloud values into a list
account_ids_list = distinct_account_ids.agg(collect_set("account_id")).first()[0]
clouds_list = distinct_clouds.agg(collect_set("cloud")).first()[0]

# Convert the list to a string
account_id_str = ','.join(account_ids_list)
cloud_str = ','.join(clouds_list)

# COMMAND ----------

# DBTITLE 1,he
# Replace these with your actual values
ACCOUNT_ID = f"{account_id_str}"  # Example: "83988469-aba5-4d3d-bcad-f8707acc74cd"
CLIENT_ID = "83988469-aba5-4d3d-bcad-f8707acc74cd"
CLIENT_SECRET = "dose1f2a6b5faf019bee3eb6e0514794d83b"

# Determine the OAuth Token Endpoint based on the cloud provider
if cloud_str == "AWS":
    token_url = f"https://accounts.cloud.databricks.com/oidc/accounts/{ACCOUNT_ID}/v1/token"
elif cloud_str == "AZURE":
    token_url = f"https://accounts.azuredatabricks.net/oidc/accounts/{ACCOUNT_ID}/v1/token"

# OAuth Request Payload
payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "all-apis",  # Required for access to all APIs
    "lifetime_seconds": 7200,  # Token expires in 2 hours
    "comment": "Workspace token for Service Principal"
}

# Headers
headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

# Request OAuth Token
response = requests.post(token_url, data=payload, headers=headers)

# Extract Token
if response.status_code == 200:
    token_data = response.json()
    ACCESS_TOKEN = token_data.get("access_token")
    print("Successfully retrieved access token!")
    # print("Access Token:", ACCESS_TOKEN)
else:
    print(f"Error obtaining access token: {response.status_code} - {response.text}")

# COMMAND ----------

# AZURE API
# Define the schema for the JSON response
schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("azure_workspace_info", MapType(StringType(), StringType(), True)),
    StructField("creation_time", LongType(), False),  # Keep as LongType to parse as timestamp later
    StructField("deployment_name", StringType(), True),
    StructField("identity_federation_info", StringType(), True),
    StructField("location", StringType(), True),
    StructField("pricing_tier", StringType(), True),
    StructField("workspace_id", LongType(), False),
    StructField("workspace_name", StringType(), True),
    StructField("workspace_status", StringType(), True),
    StructField("workspace_status_message", StringType(), True),
    StructField("network_connectivity_config_id", StringType(), True)
    # StructField("private_access_settings_id", StringType(), True),
    # StructField("storage_customer_managed_key_id", StringType(), True),
    # StructField("credentials_id", StringType(), True),
    # StructField("storage_configuration_id", StringType(), True),
])

# Run in sharing accounts
# Account region/location (e.g. US-East2)
# API endpoint to fetch all workspaces
if cloud_str == "AWS":
    api_endpoint = f"https://accounts.cloud.databricks.com/api/2.0/accounts/{ACCOUNT_ID}/workspaces"
elif cloud_str == "AZURE":
    api_endpoint = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{ACCOUNT_ID}/workspaces"

# Headers
headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# Make the GET request
response = requests.get(api_endpoint, headers=headers)

# Store the results in a Spark DataFrame
if response.status_code == 200:
    response_json = response.json()
    workspaces_data = spark.createDataFrame(response_json, schema=schema)
    
    # Alias the resulting column names and cast creation_time to timestamp
    aliased_workspaces_data = workspaces_data.select(
        col("account_id"),
        col("azure_workspace_info").alias("workspace_info"),
        from_unixtime(col("creation_time") / 1000).cast(TimestampType()).alias("creation_time"),
        col("deployment_name"),
        col("identity_federation_info"),
        col("location"),
        col("pricing_tier"),
        col("workspace_id"),
        col("workspace_name"),
        col("workspace_status"),
        col("workspace_status_message"),
        col("network_connectivity_config_id")
        # col("credentials_id").alias("credentials"),
        # col("storage_configuration_id"),
        # col("managed_services_customer_managed_key_id"),
        # col("private_access_settings_id"),
        # col("storage_customer_managed_key_id")
    )

    display(aliased_workspaces_data)
else:
    print(f"Error fetching workspaces: {response.status_code} - {response.text}")

# COMMAND ----------

# Data Inventory Overview
    # List of Databricks accounts mapped to the applications
    # Cloud provider (AWS/Azure)
query = f"""
SELECT DISTINCT 
    account_id, 
    workspace_id, 
    cloud
FROM {TARGET_CATALOG}.billing.usage
"""
data_inventory_df = spark.sql(query)
display(data_inventory_df)

# COMMAND ----------

data_inventory_overview = aliased_workspaces_data.join(data_inventory_df, on=["account_id", "workspace_id"], how="inner")

data_inventory_overview = data_inventory_overview.withColumn(
    "deployment_url",
    when(
        data_inventory_overview["cloud"] == "AZURE",
        concat(lit("https://"), data_inventory_overview["deployment_name"], lit(".azuredatabricks.net"))
    ).when(
        data_inventory_overview["cloud"] == "AWS",
        concat(lit("https://"), data_inventory_overview["deployment_name"], lit(".cloud.databricks.com"))
    )
)
display(data_inventory_overview)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
data_inventory_overview.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.workspace_details")

print("Metadata successfully stored in `metadata.workspace_details`.")

# COMMAND ----------

# Breakdown of user types 
# Fetch All Users from Databricks Account
if cloud_str == "AWS":
    users_api_url = f"https://accounts.cloud.databricks.com/api/2.0/accounts/{ACCOUNT_ID}/scim/v2/Users"
elif cloud_str == "AZURE":
    users_api_url = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{ACCOUNT_ID}/scim/v2/Users"


headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

response = requests.get(users_api_url, headers=headers)

if response.status_code == 200:
    users_data = response.json()
    print("Successfully retrieved user list!")

    # Extract user details
    user_list = []
    for user in users_data.get("Resources", []):
        user_info = {
            "user_id": user.get("id"),
            "userName": user.get("userName"),
            "displayName": user.get("displayName"),
            "active": user.get("active"),
        }

        # Process roles array
        roles = user.get("roles", [])
        if len(roles) == 1:  # If only one role, extract as individual columns
            user_info["role_type"] = roles[0].get("type")
            user_info["role_value"] = roles[0].get("value")
        else:  # If multiple roles, flatten dynamically
            for idx, role in enumerate(roles):
                user_info[f"role_type_{idx+1}"] = role.get("type")
                user_info[f"role_value_{idx+1}"] = role.get("value")

        # Process emails array
        emails = user.get("emails", [])
        if len(emails) == 1:  # If only one email, extract as individual columns
            user_info["email_type"] = emails[0].get("type")
            user_info["email_value"] = emails[0].get("value")
            user_info["email_primary"] = emails[0].get("primary")
        else:  # If multiple emails, flatten dynamically
            for idx, email in enumerate(emails):
                user_info[f"email_type_{idx+1}"] = email.get("type")
                user_info[f"email_value_{idx+1}"] = email.get("value")
                user_info[f"email_primary_{idx+1}"] = email.get("primary")

        user_list.append(user_info)

    # Convert to Spark DataFrame
    users_df = spark.createDataFrame(user_list)

    # Display the DataFrame
    display(users_df)

else:
    print(f"Error fetching users: {response.status_code} - {response.text}")


# COMMAND ----------

# # of Active users
total_users = users_df.select("user_id").distinct().count()
print(f"Total distinct users: {total_users}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Catalog Details

# COMMAND ----------

# Assets name, Assets owner, Assets description/comments Data Stewards, Tags (domain/category, confidentiality, sensitive data types, PII/PCI, business glossary association …)
# Function to safely load system tables with prefixed column names
def load_system_table(table_name, prefix):
    """Safely loads a system information schema table and adds a prefix to all columns."""
    try:
        df = spark.sql(f"SELECT * FROM {SOURCE_CATALOG}.information_schema.{table_name}")
        print(f"✅ Loaded {table_name}")
        # Add prefix to columns
        df = df.select([col(c).alias(f"{prefix}_{c}") for c in df.columns])
        df.printSchema()
        return df
    except AnalysisException:
        print(f"❌ Table {SOURCE_CATALOG}.information_schema.{table_name} not found. Skipping...")
        return None

# Load metadata tables with aliases
catalogs_df = load_system_table("catalogs", "catalogs")
catalog_tags_df = load_system_table("catalog_tags", "catalog_tags")
schemata_df = load_system_table("schemata", "schemata")
schema_tags_df = load_system_table("schema_tags", "schema_tags")
tables_df = load_system_table("tables", "tables")
table_tags_df = load_system_table("table_tags", "table_tags")
columns_df = load_system_table("columns", "columns")
column_tags_df = load_system_table("column_tags", "column_tags")

# Load privileges tables
catalog_privileges_df = load_system_table("catalog_privileges", "catalog_privileges")
schema_privileges_df = load_system_table("schema_privileges", "schema_privileges")
table_privileges_df = load_system_table("table_privileges", "table_privileges")

# Ensure required tables are loaded
if not all([catalogs_df, schemata_df, tables_df, columns_df]):
    print("❌ Critical metadata tables are missing. Aborting process.")
    exit()

# Join column_tags with columns using correct alias (`columns_column_name`)
if column_tags_df:
    columns_df = columns_df.join(
        broadcast(column_tags_df), columns_df["columns_column_name"] == column_tags_df["column_tags_column_name"], "left"
    )

# Join table_tags with tables
if table_tags_df:
    tables_df = tables_df.join(
        broadcast(table_tags_df), tables_df["tables_table_name"] == table_tags_df["table_tags_table_name"], "left"
    )

# Join columns with tables
metadata_df = columns_df.join(
    tables_df,
    (columns_df["columns_table_name"] == tables_df["tables_table_name"]) &
    (columns_df["columns_table_schema"] == tables_df["tables_table_schema"]) &
    (columns_df["columns_table_catalog"] == tables_df["tables_table_catalog"]),
    "left"
)

# Join schema_tags with schemata
if schema_tags_df:
    schemata_df = schemata_df.join(
        broadcast(schema_tags_df), schemata_df["schemata_schema_name"] == schema_tags_df["schema_tags_schema_name"], "left"
    )

# Join metadata with schemata
metadata_df = metadata_df.join(
    schemata_df,
    (metadata_df["columns_table_schema"] == schemata_df["schemata_schema_name"]) &
    (metadata_df["columns_table_catalog"] == schemata_df["schemata_catalog_name"]),
    "left"
)

# Join catalog_tags with catalogs
if catalog_tags_df:
    catalogs_df = catalogs_df.join(
        broadcast(catalog_tags_df), catalogs_df["catalogs_catalog_name"] == catalog_tags_df["catalog_tags_catalog_name"], "left"
    )

# Join metadata with catalogs
metadata_df = metadata_df.join(
    catalogs_df,
    metadata_df["columns_table_catalog"] == catalogs_df["catalogs_catalog_name"],
    "left"
)

# Join table_privileges with tables
if table_privileges_df:
    metadata_df = metadata_df.join(
        table_privileges_df,
        (metadata_df["tables_table_name"] == table_privileges_df["table_privileges_table_name"]) &
        (metadata_df["tables_table_schema"] == table_privileges_df["table_privileges_table_schema"]) &
        (metadata_df["catalogs_catalog_name"] == table_privileges_df["table_privileges_table_catalog"]),
        "left"
    )

# Join schema_privileges with schemas
if schema_privileges_df:
    metadata_df = metadata_df.join(
        schema_privileges_df,
        (metadata_df["schemata_schema_name"] == schema_privileges_df["schema_privileges_schema_name"]) &
        (metadata_df["catalogs_catalog_name"] == schema_privileges_df["schema_privileges_catalog_name"]),
        "left"
    )

# Join catalog_privileges with catalogs
if catalog_privileges_df:
    metadata_df = metadata_df.join(
        catalog_privileges_df,
        metadata_df["catalogs_catalog_name"] == catalog_privileges_df["catalog_privileges_catalog_name"],
        "left"
    )

# Drop redundant columns
metadata_df = metadata_df.drop(
    "columns_table_catalog", "columns_table_schema", "tables_table_schema", "tables_table_catalog",
    "schema_tags_catalog_name", "schema_tags_schema_name", "schemata_catalog_name", "columns_table_catalog",
    "catalog_tags_catalog_name", "table_tags_catalog_name", "table_tags_schema_name", "table_tags_table_name",
    "column_tags_catalog_name", "column_tags_schema_name", "column_tags_table_name", "column_tags_column_name",
    "catalog_privileges_catalog_name", "schema_privileges_catalog_name", "table_privileges_catalog_name",
    "schema_privileges_schema_name", "table_privileges_schema_name", "table_privileges_table_name"
)

# Drop duplicates (if needed)
metadata_df = metadata_df.dropDuplicates()

# Final Column Selection (Correct Order)
metadata_df = metadata_df.select(
    # Catalogs First
    col("catalogs_catalog_name").alias("catalog_name"),
    "catalogs_catalog_owner", "catalogs_comment", "catalogs_created", 
    "catalogs_created_by", "catalogs_last_altered", "catalogs_last_altered_by", 
    "catalog_tags_tag_name", "catalog_tags_tag_value",
    
    # Catalog Privileges
    "catalog_privileges_grantee", "catalog_privileges_privilege_type", 
    "catalog_privileges_grantor", "catalog_privileges_is_grantable", 
    "catalog_privileges_inherited_from",
    
    # Schemas Second
    col("schemata_schema_name").alias("schema_name"),
    "schemata_schema_owner", "schemata_comment", "schemata_created", 
    "schemata_created_by", "schemata_last_altered", "schemata_last_altered_by",
    "schema_tags_tag_name", "schema_tags_tag_value",
    
    # Schema Privileges
    "schema_privileges_grantee", "schema_privileges_privilege_type", 
    "schema_privileges_grantor", "schema_privileges_is_grantable", 
    "schema_privileges_inherited_from",
    
    # Tables Third
    col("tables_table_name").alias("table_name"),
    "tables_table_owner", "tables_table_type", "tables_comment", "tables_created",
    "tables_created_by", "tables_last_altered", "tables_last_altered_by",
    "table_tags_tag_name", "table_tags_tag_value",
    
    # Table Privileges
    "table_privileges_grantee", "table_privileges_privilege_type", 
    "table_privileges_grantor", "table_privileges_is_grantable", 
    "table_privileges_inherited_from",
    
    # Columns Last
    col("columns_column_name").alias("column_name"),
    "columns_data_type", "columns_comment", "columns_is_nullable", 
    "columns_character_maximum_length", "columns_numeric_precision", 
    "columns_numeric_scale", "columns_datetime_precision",
    "column_tags_tag_name", "column_tags_tag_value"
)

# Display results
display(metadata_df)

# COMMAND ----------

# **Filter out `information_schema` and system views**
filtered_asset_details_df = metadata_df.filter(
    (col("schema_name") != "information_schema") & 
    (~col("catalog_name").startswith("__databricks_internal"))
).select("catalog_name", "schema_name", "table_name").dropDuplicates()

# **Extract unique table list**
table_list = [
    (row["catalog_name"], row["schema_name"], row["table_name"])
    for row in filtered_asset_details_df.select("catalog_name", "schema_name", "table_name").distinct().collect()
]

print(f"📊 Unique tables to process: {len(table_list)}")

# **Track failed tables to avoid duplicate errors**
failed_tables = set()

# **Function to process a single table**
def get_table_metadata(catalog_name, schema_name, table_name):
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    if full_table_name in failed_tables:
        return None  # Skip already failed tables

    try:
        # **Verify table accessibility**
        check_table_query = f"SHOW TABLES IN {catalog_name}.{schema_name} LIKE '{table_name}'"
        if spark.sql(check_table_query).count() == 0:
            print(f"⚠️ {full_table_name} exists but is not accessible. Skipping...")
            failed_tables.add(full_table_name)
            return None

        # **Run DESCRIBE DETAIL**
        detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
        detail_row = detail_df.collect()[0]

        # **Extract size in bytes (handle None)**
        size_in_bytes = detail_row["sizeInBytes"] if detail_row["sizeInBytes"] else 0
        size_in_gb = round(size_in_bytes / (1024 ** 3), 3)
        size_in_mb = round(size_in_bytes / (1024 ** 2), 3)

        # **Get row count**
        row_count_query = f"SELECT COUNT(*) AS count FROM {full_table_name}"
        row_count = spark.sql(row_count_query).collect()[0]["count"]

        return (catalog_name, schema_name, table_name, size_in_bytes, size_in_gb, size_in_mb, row_count)

    except AnalysisException:
        print(f"❌ Table {full_table_name} not found. Skipping...")
        failed_tables.add(full_table_name)
    except Exception as e:
        print(f"⚠️ Error processing {full_table_name}: {str(e)}")
    
    return None  # Return None for failed cases

# **Parallel Processing with ThreadPoolExecutor**
size_metadata_list = []
MAX_WORKERS = 8  # Adjust based on cluster capacity

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_table = {executor.submit(get_table_metadata, cat, sch, tbl): (cat, sch, tbl) for cat, sch, tbl in table_list}

    for future in as_completed(future_to_table):
        result = future.result()
        if result:
            size_metadata_list.append(result)

# **Check if we have data before creating DataFrame**
if size_metadata_list:
    size_columns = ["catalog_name", "schema_name", "table_name", "size_in_bytes", "size_in_gb", "size_in_mb", "row_count"]
    size_metadata_df = spark.createDataFrame(size_metadata_list, size_columns)

    # **Join with metadata_df**
    table_size_rowcount_df = filtered_asset_details_df.join(size_metadata_df, ["catalog_name", "schema_name", "table_name"], "left")

    # **Display the final DataFrame**
    display(table_size_rowcount_df)
else:
    print("⚠️ No valid table metadata found. Possible causes: permissions, missing tables, or failed queries.")


# COMMAND ----------

# **Filter out `information_schema` and system views**
filtered_asset_details_df = metadata_df.filter(
    (col("schema_name") != "information_schema") & 
    (~col("catalog_name").startswith("__databricks_internal"))
).select("catalog_name", "schema_name", "table_name").dropDuplicates()

# **Extract unique table list**
table_list = [
    (row["catalog_name"], row["schema_name"], row["table_name"])
    for row in filtered_asset_details_df.collect()
]

print(f"📊 Unique tables to process: {len(table_list)}")

# **Track failed tables to avoid duplicate errors**
failed_tables = set()

# **Function to fetch `SHOW TBLPROPERTIES` for a table**
def get_table_properties(catalog_name, schema_name, table_name):
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

    if full_table_name in failed_tables:
        return None  # Skip already failed tables

    try:
        # **Verify table accessibility**
        check_table_query = f"SHOW TABLES IN {catalog_name}.{schema_name} LIKE '{table_name}'"
        if spark.sql(check_table_query).count() == 0:
            print(f"⚠️ {full_table_name} exists but is not accessible. Skipping...")
            failed_tables.add(full_table_name)
            return None

        # **Fetch Table Properties**
        properties_df = spark.sql(f"SHOW TBLPROPERTIES {full_table_name}")

        # **Convert properties into a dictionary**
        properties_dict = {row["key"]: row["value"] for row in properties_df.collect()}

        return (catalog_name, schema_name, table_name, properties_dict)

    except AnalysisException:
        print(f"❌ Table {full_table_name} not found. Skipping...")
        failed_tables.add(full_table_name)
    except Exception as e:
        print(f"⚠️ Error processing {full_table_name}: {str(e)}")

    return None  # Return None for failed cases

# **Parallel Processing with ThreadPoolExecutor**
tbl_properties_list = []
MAX_WORKERS = 8  # Adjust based on cluster capacity

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_table = {executor.submit(get_table_properties, cat, sch, tbl): (cat, sch, tbl) for cat, sch, tbl in table_list}

    for future in as_completed(future_to_table):
        result = future.result()
        if result:
            tbl_properties_list.append(result)

# **Check if we have data before creating DataFrame**
if tbl_properties_list:
    # Convert to DataFrame
    properties_columns = ["catalog_name", "schema_name", "table_name", "tbl_properties"]
    tbl_properties_df = spark.createDataFrame(tbl_properties_list, properties_columns)

    # Convert MAP to ARRAY<STRUCT<key: STRING, value: STRING>>
    tbl_properties_df = tbl_properties_df.withColumn("properties", map_entries(col("tbl_properties")))

    # Flatten the `tbl_properties` column (convert to key-value columns)
    exploded_tbl_properties_df = (
        tbl_properties_df
        .select("catalog_name", "schema_name", "table_name", col("properties"))
        .selectExpr("catalog_name", "schema_name", "table_name", "inline(properties)")  # Expands key-value pairs
        .groupby("catalog_name", "schema_name", "table_name")
        .agg(collect_list("key").alias("property_keys"), collect_list("value").alias("property_values"))
    )

    # **Join with `final_metadata_df`**
    enriched_metadata_df = final_metadata_df.join(
        exploded_tbl_properties_df, ["catalog_name", "schema_name", "table_name"], "left"
    )

    # **Display the final DataFrame**
    display(enriched_metadata_df)

else:
    print("⚠️ No valid table properties found. Possible causes: permissions, missing tables, or failed queries.")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
final_metadata_df.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.asset_details")

print("Metadata successfully stored in `metadata.asset_details`.")

# COMMAND ----------

# Define the access token (ensure this is securely managed)
ACCESS_TOKEN = f"{ACCESS_TOKEN}"

# Read the clusters table into a DataFrame
clusters_df = spark.table(f"{SOURCE_CATALOG}.compute.clusters")

# Select distinct cluster_id and workspace_id values
distinct_cluster_ids = clusters_df.select("cluster_id", "workspace_id").distinct()

# Assume data_inventory_overview is already available as a DataFrame
# Join distinct cluster IDs with data_inventory_overview to get workspace details
joined_df = distinct_cluster_ids.join(data_inventory_overview, on="workspace_id", how="inner")

# Define a function to call the Databricks REST API
def get_cluster_info(workspace_url, cluster_id):
    endpoint = f"{workspace_url}/api/2.1/clusters/get?cluster_id={cluster_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Status code {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

# Register the function as a UDF
get_cluster_info_udf = udf(get_cluster_info, MapType(StringType(), StringType()))

# Add a column with cluster information from the API
result_df = joined_df.withColumn("cluster_info", get_cluster_info_udf(col("deployment_url"), col("cluster_id")))

# Extract keys from the cluster_info map
keys_df = result_df.select(explode(map_keys(col("cluster_info")))).distinct()
keys_list = [row[0] for row in keys_df.collect()]

# Create columns for each key in cluster_info
for key in keys_list:
    result_df = result_df.withColumn(key, col("cluster_info").getItem(key))

# Select relevant columns
filtered_df = result_df.select("workspace_id", "deployment_url", "cluster_id", *keys_list)

# Remove rows where cluster_id is null
cluster_info_id_df = filtered_df.filter(col("cluster_id").isNotNull())

# Display the resulting DataFrame
display(cluster_info_id_df)


# COMMAND ----------

import requests
from pyspark.sql.functions import col, lit, udf, explode, map_keys
from pyspark.sql.types import MapType, StringType

# Read the clusters table into a DataFrame
clusters_df = spark.table(f"{SOURCE_CATALOG}.compute.clusters")

# Select distinct cluster_id and workspace_id values
distinct_cluster_ids = clusters_df.select("cluster_id", "workspace_id").distinct()

# Assume data_inventory_overview is already available as a DataFrame
# Join distinct cluster IDs with data_inventory_overview to get workspace details
joined_df = distinct_cluster_ids.join(data_inventory_overview, on="workspace_id", how="inner")

# Define a function to call the Databricks REST API to get cluster info
def get_cluster_info(workspace_url, cluster_id):
    endpoint = f"{workspace_url}/api/2.1/clusters/get?cluster_id={cluster_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Status code {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

# Register the function as a UDF
get_cluster_info_udf = udf(get_cluster_info, MapType(StringType(), StringType()))

# Add a column with cluster information from the API
result_df = joined_df.withColumn("cluster_info", get_cluster_info_udf(col("deployment_url"), col("cluster_id")))

# Extract keys from the cluster_info map
keys_df = result_df.select(explode(map_keys(col("cluster_info")))).distinct()
keys_list = [row[0] for row in keys_df.collect()]

# Add the policy_id column with a default value if it's missing
if 'policy_id' not in keys_list:
    result_df = result_df.withColumn('policy_id', lit(None))

# Create columns for each key in cluster_info
for key in keys_list:
    result_df = result_df.withColumn(key, col("cluster_info").getItem(key))

# Select relevant columns
filtered_df = result_df.select("workspace_id", "deployment_url", "cluster_id", *keys_list)

# Remove rows where cluster_id is null
cluster_info_id_df = filtered_df.filter(col("cluster_id").isNotNull())

# Define a function to call the Databricks REST API to get policy info
def get_policy_info(workspace_url, policy_id, access_token):
    if not policy_id:
        return {"policy_id": None, "name": None, "definition": None, "description": None}
    endpoint = f"{workspace_url}/api/2.0/policies/clusters/get?policy_id={policy_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Status code {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}


# Register the function as a UDF
get_policy_info_udf = udf(get_policy_info, MapType(StringType(), StringType()))

# Add a column with policy information from the API
final_df = cluster_info_id_df.withColumn("policy_info", get_policy_info_udf(col("deployment_url"), col("policy_id")))

# Extract policy_name from the policy_info map
final_df = final_df.withColumn("policy_name", col("policy_info").getItem("policy_name"))

# Select relevant columns
final_df = final_df #.select("workspace_id", "deployment_url", "cluster_id", "cluster_name", "policy_id", "policy_name")

# Display the resulting DataFrame
display(final_df)


# COMMAND ----------

import requests
from pyspark.sql.functions import col, lit, udf, explode, map_keys
from pyspark.sql.types import MapType, StringType

# Define the access token (ensure this is securely managed)
ACCESS_TOKEN = f"{ACCESS_TOKEN}"

# Read the clusters table into a DataFrame
clusters_df = spark.table("cog_land_system.compute.clusters")

# Select distinct cluster_id and workspace_id values
distinct_cluster_ids = clusters_df.select("cluster_id", "workspace_id").distinct()

# Assume data_inventory_overview is already available as a DataFrame
# Join distinct cluster IDs with data_inventory_overview to get workspace details
joined_df = distinct_cluster_ids.join(data_inventory_overview, on="workspace_id", how="inner")

# Define a function to call the Databricks REST API to get cluster info
def get_cluster_info(workspace_url, cluster_id):
    endpoint = f"{workspace_url}/api/2.1/clusters/get?cluster_id={cluster_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Status code {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

# Register the function as a UDF
get_cluster_info_udf = udf(get_cluster_info, MapType(StringType(), StringType()))

# Add a column with cluster information from the API
result_df = joined_df.withColumn("cluster_info", get_cluster_info_udf(col("deployment_url"), col("cluster_id")))

# Extract keys from the cluster_info map
keys_df = result_df.select(explode(map_keys(col("cluster_info")))).distinct()
keys_list = [row[0] for row in keys_df.collect()]

# Add the policy_id column with a default value if it's missing
if 'policy_id' not in keys_list:
    result_df = result_df.withColumn('policy_id', lit(None))

# Create columns for each key in cluster_info
for key in keys_list:
    result_df = result_df.withColumn(key, col("cluster_info").getItem(key))

# Select relevant columns
filtered_df = result_df.select("workspace_id", "deployment_url", "cluster_id", *keys_list)

# Remove rows where cluster_id is null
cluster_info_id_df = filtered_df.filter(col("cluster_id").isNotNull())

# Define a function to call the Databricks REST API to get policy info
def get_policy_info(workspace_url, policy_id):
    if policy_id is None:
        return {"policy_name": None, "policy_definition": None, "policy_description": None}
    endpoint = f"{workspace_url}/api/2.0/policies/clusters/get?policy_id={policy_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            policy_info = response.json()
            return {
                "policy_name": policy_info.get("name", None),
                "policy_definition": policy_info.get("definition", None),
                "policy_description": policy_info.get("description", None)
            }
        else:
            return {"policy_name": None, "policy_definition": None, "policy_description": None}
    except Exception as e:
        return {"policy_name": None, "policy_definition": None, "policy_description": None}

# Register the function as a UDF
get_policy_info_udf = udf(get_policy_info, MapType(StringType(), StringType()))

# Add a column with policy information from the API
final_df = cluster_info_id_df.withColumn("policy_info", get_policy_info_udf(col("deployment_url"), col("policy_id")))

# Extract policy_name, policy_definition, and policy_description from the policy_info map
final_df = final_df.withColumn("policy_name", col("policy_info").getItem("policy_name"))
final_df = final_df.withColumn("policy_definition", col("policy_info").getItem("policy_definition"))
final_df = final_df.withColumn("policy_description", col("policy_info").getItem("policy_description"))

# Select relevant columns
# final_df = final_df.select("workspace_id", "deployment_url", "cluster_id", "cluster_name", "policy_id", "policy_name", "policy_definition", "policy_description")

# Display the resulting DataFrame
display(final_df)


# COMMAND ----------

import requests
from pyspark.sql.functions import col, lit, udf, explode, map_keys
from pyspark.sql.types import MapType, StringType, IntegerType, BooleanType, ArrayType

# Define the access token (ensure this is securely managed)
ACCESS_TOKEN = f"{ACCESS_TOKEN}"

# Read the clusters table into a DataFrame
clusters_df = spark.table(f"{SOURCE_CATALOG}.compute.clusters")

# Select distinct cluster_id and workspace_id values
distinct_cluster_ids = clusters_df.select("cluster_id", "workspace_id").distinct()

# Assume data_inventory_overview is already available as a DataFrame
# Join distinct cluster IDs with data_inventory_overview to get workspace details
joined_df = distinct_cluster_ids.join(data_inventory_overview, on="workspace_id", how="inner")

# Define a function to call the Databricks REST API to get cluster info
def get_cluster_info(workspace_url, cluster_id):
    endpoint = f"{workspace_url}/api/2.1/clusters/get?cluster_id={cluster_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Status code {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

# Register the function as a UDF
get_cluster_info_udf = udf(get_cluster_info, MapType(StringType(), StringType()))

# Add a column with cluster information from the API
result_df = joined_df.withColumn("cluster_info", get_cluster_info_udf(col("deployment_url"), col("cluster_id")))

# Extract keys from the cluster_info map
keys_df = result_df.select(explode(map_keys(col("cluster_info")))).distinct()
keys_list = [row[0] for row in keys_df.collect()]

# Add the policy_id column with a default value if it's missing
if 'policy_id' not in keys_list:
    result_df = result_df.withColumn('policy_id', lit(None))

# Create columns for each key in cluster_info
for key in keys_list:
    result_df = result_df.withColumn(key, col("cluster_info").getItem(key))

# Select relevant columns
filtered_df = result_df.select("workspace_id", "deployment_url", "cluster_id", *keys_list)

# Remove rows where cluster_id is null
cluster_info_id_df = filtered_df.filter(col("cluster_id").isNotNull())

# Define a function to call the Databricks REST API to get policy info
def get_policy_info(workspace_url, policy_id):
    if policy_id is None:
        return {
            "policy_name": None,
            "policy_definition": None,
            "policy_description": None,
            "max_clusters_per_user": None,
            "libraries": None,
            "creator_user_name": None,
            "created_at_timestamp": None,
            "is_default": None
        }
    endpoint = f"{workspace_url}/api/2.0/policies/clusters/get?policy_id={policy_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            policy_info = response.json()
            return {
                "policy_name": policy_info.get("name", None),
                "policy_definition": policy_info.get("definition", None),
                "policy_description": policy_info.get("description", None),
                "max_clusters_per_user": policy_info.get("max_clusters_per_user", None),
                "libraries": policy_info.get("libraries", None),
                "creator_user_name": policy_info.get("creator_user_name", None),
                "created_at_timestamp": policy_info.get("created_at_timestamp", None),
                "is_default": policy_info.get("is_default", None)
            }
        else:
            return {
                "policy_name": None,
                "policy_definition": None,
                "policy_description": None,
                "max_clusters_per_user": None,
                "libraries": None,
                "creator_user_name": None,
                "created_at_timestamp": None,
                "is_default": None
            }
    except Exception as e:
        return {
            "policy_name": None,
            "policy_definition": None,
            "policy_description": None,
            "max_clusters_per_user": None,
            "libraries": None,
            "creator_user_name": None,
            "created_at_timestamp": None,
            "is_default": None
        }

# Register the function as a UDF
get_policy_info_udf = udf(get_policy_info, MapType(StringType(), StringType()))

# Add a column with policy information from the API
final_df = cluster_info_id_df.withColumn("policy_info", get_policy_info_udf(col("deployment_url"), col("policy_id")))

# Extract policy fields from the policy_info map
policy_fields = [
    "policy_name",
    "policy_definition",
    "policy_description",
    "max_clusters_per_user",
    "libraries",
    "creator_user_name",
    "created_at_timestamp",
    "is_default"
]

for field in policy_fields:
    final_df = final_df.withColumn(field, col("policy_info").getItem(field))

# Select relevant columns
final_df = final_df#.select(
#     "workspace_id",
#     "deployment_url",
#     "cluster_id",
#     "cluster_name",
#     "policy_id",
#     "policy_name",
#     "policy_definition",
#     "policy_description",
#     "max_clusters_per_user",
#     "libraries",
#     "creator_user_name",
#     "created_at_timestamp",
#     "is_default"
# )

# Display the resulting DataFrame
display(final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC # dev testing zone do not run

# COMMAND ----------

query = """
SELECT *
FROM system.access.audit
where workspace_id != 0
"""
df = spark.sql(query)
display(df)

# COMMAND ----------

query = """
SELECT DISTINCT user_identity.email as user_identity, account_id, workspace_id
FROM system.access.audit
"""
df = spark.sql(query)
unique_user_identities = df.select("user_identity").distinct().collect()
display(df)

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import explode, col, concat_ws, lit

# Step 2: Fetch All Users from Databricks Account
users_api_url = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{ACCOUNT_ID}/scim/v2/Users"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

response = requests.get(users_api_url, headers=headers)

if response.status_code == 200:
    users_data = response.json()
    print("✅ Successfully retrieved user list!")

    # Extract user details
    user_list = []
    for user in users_data.get("Resources", []):
        user_list.append({
            "user_id": user.get("id"),
            "userName": user.get("userName"),
            "displayName": user.get("displayName"),
            "active": user.get("active"),
            "roles": user.get("roles", []),
            "emails": user.get("emails", [])
        })

    # Convert to Spark DataFrame
    users_df = spark.createDataFrame(user_list)

    # Explode roles and emails arrays into separate rows
    users_df = users_df.withColumn("role", explode(col("roles"))) \
                       .withColumn("email", explode(col("emails")))

    # Extract type and value from roles
    users_df = users_df.withColumn("role_type", col("role.type")) \
                       .withColumn("role_value", col("role.value"))

    # Handle multiple elements in the array
    users_df = users_df.withColumn("role_type", concat_ws(",", col("role_type"))) \
                       .withColumn("role_value", concat_ws(",", col("role_value")))

    # Drop the original role column
    users_df = users_df.drop("role")

    # Display the DataFrame
    display(users_df)

else:
    print(f"❌ Error fetching users: {response.status_code} - {response.text}")

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit

# Fetch All Users from Databricks Account
users_api_url = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{ACCOUNT_ID}/scim/v2/Users"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

response = requests.get(users_api_url, headers=headers)

if response.status_code == 200:
    users_data = response.json()
    print("✅ Successfully retrieved user list!")

    # Extract user details
    user_list = []
    for user in users_data.get("Resources", []):
        user_list.append({
            "user_id": user.get("id"),
            "userName": user.get("userName"),
            "displayName": user.get("displayName"),
            "active": user.get("active"),
            "roles": [role.get("value") for role in user.get("roles", [])],  # Extract role values
            "emails": [email.get("value") for email in user.get("emails", [])]  # Extract email values
        })

    # Convert to Spark DataFrame
    users_df = spark.createDataFrame(user_list)

    # Explode roles and emails into separate rows
    users_df = users_df.withColumn("role", explode(col("roles")))
    users_df = users_df.withColumn("email", explode(col("emails")))

    # Drop original array columns
    users_df = users_df.drop("roles", "emails")

    # Display the DataFrame
    display(users_df)

else:
    print(f"❌ Error fetching users: {response.status_code} - {response.text}")


# COMMAND ----------

import requests
import json
from pyspark.sql.functions import col

# Step 1: Extract Unique User Identities from system.access.audit Table
query = """
SELECT DISTINCT user_identity.email as user_identity
FROM system.access.audit
"""
df = spark.sql(query)
unique_user_identities = [row["user_identity"] for row in df.select("user_identity")
                          .distinct().collect()]
print(f"Extracted {len(unique_user_identities)} unique user identities.")

# Step 2: Fetch Databricks API Authentication Details
DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext()\
                                                            .apiUrl().getOrElse(None)
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext()\
                                                            .apiToken().getOrElse(None)

# SCIM API Endpoint for Users
api_endpoint = f"{DATABRICKS_HOST}/api/2.0/preview/scim/v2/Users"

# Headers for API Requests
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Step 3: Fetch User IDs for the Extracted Emails
user_id_map = {}  # Dictionary to store email -> user ID mapping

for email in unique_user_identities:
    params = {
        "filter": f"userName eq \"{email}\"",  # Search by exact email match
        "attributes": "id,userName"
    }

    response = requests.get(api_endpoint, headers=headers, params=params)

    if response.status_code == 200:
        users = response.json().get("Resources", [])
        if users:
            user_id_map[email] = users[0]["id"]
            print(f"Found User ID for {email}: {users[0]['id']}")
        else:
            print(f"User not found in SCIM for {email}.")
    else:
        print(f"Error fetching user ID for {email}: {response.status_code} - {response.text}")

# Step 4: Fetch Detailed User Information for Each User ID
account_id = "your_account_id"  # Replace with the actual account ID
user_details_list = []  # List to store user details

for email, user_id in user_id_map.items():
    user_url = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Users/{user_id}"

    response = requests.get(user_url, headers=headers)
    
    if response.status_code == 200:
        user_details = response.json()
        user_details_list.append(user_details)
        print(f"Retrieved details for user {email}: {json.dumps(user_details, indent=2)}")
    else:
        print(f"Error fetching details for {email} (User ID: {user_id}): {response.status_code} - {response.text}")

# Display the final list of user details
display(user_details_list)


# COMMAND ----------

import requests
import json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col

# Step 1: Extract Unique User Identities and Account IDs from system.access.audit
query = """
SELECT DISTINCT user_identity.email AS user_identity, account_id
FROM system.access.audit
"""
df = spark.sql(query)
user_account_pairs = df.select("user_identity", "account_id").distinct().collect()

# Convert to a list of tuples (user_identity, account_id)
user_account_list = [(row["user_identity"], row["account_id"]) for row in user_account_pairs]
print(f"Extracted {len(user_account_list)} unique (user, account) pairs.")

# Step 2: Fetch Databricks API Authentication Details
DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# SCIM API Endpoint for Users
api_endpoint = f"{DATABRICKS_HOST}/api/2.0/preview/scim/v2/Users"

# Headers for API Requests
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Step 3: Fetch User IDs for Each Email and Validate Account ID
validated_users = []  # List to store validated user details

def is_valid_email(email):
    """Checks if a string is a valid email (not a UUID)."""
    return "@" in email and "." in email  # Simple email validation

for email, account_id in user_account_list:
    if not is_valid_email(email):
        print(f"⚠️ Skipping invalid email format: {email}")
        continue

    params = {
        "filter": f"userName eq \"{email}\"",  # Search by exact email match
        "attributes": "id,userName"
    }

    response = requests.get(api_endpoint, headers=headers, params=params)

    if response.status_code == 200:
        response_data = response.json()
        users = response_data.get("Resources", [])

        if users:
            user_id = users[0]["id"]  # Extract user ID
            print(f"✅ Found User ID for {email}: {user_id} (Account: {account_id})")

            # Step 4: Validate the Account ID & User ID Pair
            user_url = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Users/{user_id}"
            user_response = requests.get(user_url, headers=headers)

            if user_response.status_code == 200:
                user_details = user_response.json()
                validated_users.append(Row(
                    email=email,
                    account_id=account_id,
                    user_id=user_id,
                    user_details=json.dumps(user_details)
                ))
                print(f"✅ Validated User {email} for Account {account_id}")
            else:
                print(f"❌ User {email} does not belong to Account {account_id}: {user_response.status_code}")
        else:
            print(f"⚠️ No User ID found for {email}")
    else:
        print(f"Error fetching user ID for {email}: {response.status_code} - {response.text}")

# Step 5: Create a Spark DataFrame for Validated Users
if validated_users:
    schema = StructType([
        StructField("email", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_details", StringType(), True)  # Store JSON as a string
    ])
    validated_users_df = spark.createDataFrame(validated_users, schema)
    display(validated_users_df)
else:
    print("⚠️ No validated users found.")


# COMMAND ----------

import requests

# Replace with your actual values
CLIENT_ID = "83988469-aba5-4d3d-bcad-f8707acc74cd"
CLIENT_SECRET = "dose1f2a6b5faf019bee3eb6e0514794d83b"
DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)

# OAuth Token URL (Workspace Level)
token_url = f"{DATABRICKS_HOST}/api/2.0/token/create"

# Request Headers
headers = {
    "Authorization": f"Basic {CLIENT_ID}:{CLIENT_SECRET}",
    "Content-Type": "application/json"
}

# Request Payload (Token Request)
payload = {
    "lifetime_seconds": 3600,  # 1 hour token
    "comment": "Token for Service Principal"
}

# Make the POST request
response = requests.post(token_url, json=payload, headers=headers)

# Handle Response
if response.status_code == 200:
    token_data = response.json()
    ACCESS_TOKEN = token_data.get("token_value")
    print("✅ Successfully retrieved access token!")
    print("🔑 Access Token:", ACCESS_TOKEN)
else:
    print(f"❌ Error obtaining access token: {response.status_code} - {response.text}")


# COMMAND ----------

# Step 2: Use OAuth Token to Create a Workspace Access Token
token_create_url = f"{DATABRICKS_HOST}/api/2.0/token/create"

headers = {
    "Authorization": f"Bearer {oauth_token}",
    "Content-Type": "application/json"
}

payload = {
    "lifetime_seconds": 3600,  # Token expires in 1 hour
    "comment": "Workspace token for Service Principal"
}

# Request a new workspace access token
token_response = requests.post(token_create_url, json=payload, headers=headers)

if token_response.status_code == 200:
    workspace_token = token_response.json().get("token_value")
    print("✅ Successfully created a workspace access token for Service Principal!")
    print("🔑 New Access Token:", workspace_token)
else:
    print(f"❌ Error creating workspace access token: {token_response.status_code} - {token_response.text}")

