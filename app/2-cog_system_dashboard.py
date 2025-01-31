# Databricks notebook source
# MAGIC %md
# MAGIC # Import libs

# COMMAND ----------

import re
import json
import time
import requests
from pyspark.sql.types import (MapType, StringType, IntegerType, LongType, TimestampType, 
                               DateType, DecimalType, BinaryType, StructType, StructField, 
                               BooleanType, ArrayType)
from pyspark.sql.functions import (collect_set, concat, lit, when, col, explode, broadcast, 
                                   collect_list, map_entries, udf, map_keys, from_unixtime)
from pyspark.sql.utils import AnalysisException
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter widgets

# COMMAND ----------

# Inspect the schema to find the correct column name
providers_df = spark.sql("SHOW PROVIDERS")

# Assuming the correct column name is 'provider'
providers = [row.name for row in providers_df.collect()]

shares = []
for provider in providers:
    shares_df = spark.sql(f"SHOW SHARES IN PROVIDER `{provider}`")
    shares.extend([row.name for row in shares_df.collect()])

# # Convert the list of shares to a DataFrame
try:
    shares = spark.createDataFrame([(share,) for share in shares], ["share_name"]).collect()
except ValueError:
    print("No shares found.")

# COMMAND ----------

dbutils.widgets.text("SOURCE_CATALOG", "system")
dbutils.widgets.text("TARGET_CATALOG", "cog_land_system")
dbutils.widgets.text("INFO_SCHEMA_TARGET", "cog_information_schema")
dbutils.widgets.text("EXCLUDED_SCHEMAS", "ai")
dbutils.widgets.text("EXCLUDED_TABLES", "_sqldf,__internal_logging")
dbutils.widgets.text("CLIENT_ID", "<my_client_id>") # add your CLIENT_ID 

SOURCE_CATALOG = dbutils.widgets.get("SOURCE_CATALOG")
TARGET_CATALOG = dbutils.widgets.get("TARGET_CATALOG")
INFO_SCHEMA_TARGET = dbutils.widgets.get("INFO_SCHEMA_TARGET")
EXCLUDED_SCHEMAS = set(dbutils.widgets.get("EXCLUDED_SCHEMAS").split(","))
EXCLUDED_TABLES = set(dbutils.widgets.get("EXCLUDED_TABLES").split(","))
CLIENT_ID = dbutils.widgets.get("CLIENT_ID")

# **Convert `excluded_shares` to a set for fast lookup**
EXCLUDED_SHARES = set(shares)

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

# Replace these with your actual values
ACCOUNT_ID = f"{account_id_str}" 
CLIENT_SECRET = dbutils.secrets.get(scope = "cog_system", key = "accnt_admin_sp")

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

# API
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

    # display(aliased_workspaces_data)
else:
    print(f"Error fetching workspaces: {response.status_code} - {response.text}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
aliased_workspaces_data.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.workspaces_status")

print("Metadata successfully stored in `metadata.workspaces_status`.")

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
# display(data_inventory_df)

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

workspace_url_list = data_inventory_overview.select("deployment_url").distinct()

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
data_inventory_overview.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.running_workspace_details")

print("Metadata successfully stored in `metadata.running_workspace_details`.")

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

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
users_df.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.users_df")

print("Metadata successfully stored in `metadata.users_df`.")

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

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
metadata_df.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.assets_metadata")

print("Metadata successfully stored in `metadata.assets_metadata`.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Asset Properties 

# COMMAND ----------

# Convert EXCLUDED_SCHEMAS, EXCLUDED_TABLES to Python lists
EXCLUDED_SCHEMAS = list(EXCLUDED_SCHEMAS)
EXCLUDED_TABLES = list(EXCLUDED_TABLES)

# **Extract share_name directly from Row objects**
EXCLUDED_SHARES = [row.share_name for row in EXCLUDED_SHARES]  # Extracting the correct field

def apply_exclusions(df, schema_col="schema_name", table_col="table_name", catalog_col="catalog_name"):
    return df.filter(
        (~col(schema_col).isin(EXCLUDED_SCHEMAS)) &
        (~col(table_col).isin(EXCLUDED_TABLES)) &
        (~col(catalog_col).isin(EXCLUDED_SHARES))  # filtering shares
    )

# **Usage**
filtered_df = apply_exclusions(metadata_df)

# COMMAND ----------

# **Filter out `information_schema` and system views**
filtered_asset_details_df = filtered_df.filter(
    (col("schema_name") != "information_schema") & 
    (~col("catalog_name").isin("__databricks_internal","system"))
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
filtered_asset_details_df = filtered_df.filter(
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
    metadata_tblproperties_df = table_size_rowcount_df.join(
        exploded_tbl_properties_df, ["catalog_name", "schema_name", "table_name"], "left"
    )

    # **Display the final DataFrame**
    display(metadata_tblproperties_df)

else:
    print("⚠️ No valid table properties found. Possible causes: permissions, missing tables, or failed queries.")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
metadata_tblproperties_df.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.metadata_tblproperties")

print("Metadata successfully stored in `metadata.metadata_tblproperties`.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute Policies

# COMMAND ----------

if cloud_str.upper() == "AZURE":
    print("✅ Running in Azure Databricks. Executing API calls...")
    
    # **Define the access token**
    ACCESS_TOKEN = f"{ACCESS_TOKEN}"

    # **Read Clusters Table into DataFrame**
    clusters_df = spark.table(f"{SOURCE_CATALOG}.compute.clusters")

    # **Select Unique cluster_id & workspace_id**
    distinct_cluster_ids = clusters_df.select("cluster_id", "workspace_id").distinct()

    # **Join with Deployment Details**
    joined_df = distinct_cluster_ids.join(data_inventory_overview, on="workspace_id", how="inner")

    # **Function to Fetch Cluster Info from API**
    def get_cluster_info(workspace_url, cluster_id):
        endpoint = f"{workspace_url}/api/2.1/clusters/get?cluster_id={cluster_id}"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        try:
            response = requests.get(endpoint, headers=headers)
            return response.json() if response.status_code == 200 else {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return {"error": str(e)}

    # **Function to Fetch Policy Info from API**
    def get_policy_info(workspace_url, policy_id):
        if not policy_id:
            return {
                "policy_name": None, "policy_definition": None, "policy_description": None,
                "max_clusters_per_user": None, "libraries": None, "creator_user_name": None,
                "created_at_timestamp": None, "is_default": None
            }
        endpoint = f"{workspace_url}/api/2.0/policies/clusters/get?policy_id={policy_id}"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        try:
            response = requests.get(endpoint, headers=headers)
            return response.json() if response.status_code == 200 else {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return {"error": str(e)}

    # **Register UDFs for API Calls**
    get_cluster_info_udf = udf(get_cluster_info, MapType(StringType(), StringType()))
    get_policy_info_udf = udf(get_policy_info, MapType(StringType(), StringType()))

    # **Fetch Cluster & Policy Data**
    result_df = joined_df.withColumn("cluster_info", get_cluster_info_udf(col("deployment_url"), col("cluster_id")))

    # **Extract Keys from cluster_info**
    keys_df = result_df.select(explode(map_keys(col("cluster_info")))).distinct()
    keys_list = [row[0] for row in keys_df.collect()]

    # **Add Missing `policy_id` Column**
    if 'policy_id' not in keys_list:
        result_df = result_df.withColumn('policy_id', lit(None))

    # **Extract Cluster Fields**
    for key in keys_list:
        result_df = result_df.withColumn(key, col("cluster_info").getItem(key))

    # **Remove Rows Where `cluster_id` is NULL**
    filtered_df = result_df.select("workspace_id", "deployment_url", "cluster_id", *keys_list)
    cluster_info_id_df = filtered_df.filter(col("cluster_id").isNotNull())

    # **Fetch Policy Information**
    final_df = cluster_info_id_df.withColumn("policy_info", get_policy_info_udf(col("deployment_url"), col("policy_id")))

    # **Extract Policy Fields**
    policy_fields = ["policy_name", "policy_definition", "policy_description", "max_clusters_per_user", 
                    "libraries", "creator_user_name", "created_at_timestamp", "is_default"]

    for field in policy_fields:
        final_df = final_df.withColumn(field, col("policy_info").getItem(field))

    # **List of Expected Columns**
    expected_columns = [
        "workspace_id", "deployment_url", "cluster_id", "runtime_engine", "driver_node_type_id",
        "cluster_source", "last_state_loss_time", "creator_user_name", "node_type_id", "state_message",
        "cluster_cores", "instance_source", "last_activity_time", "single_user_name", "cluster_name",
        "cluster_memory_mb", "spark_version", "spark_conf", "default_tags", "cluster_log_conf",
        "start_time", "jdbc_port", "spark_env_vars", "enable_local_disk_encryption",
        "init_scripts_safe_mode", "init_scripts", "assigned_principal", "enable_elastic_disk", "disk_spec",
        "azure_attributes", "error", "driver_instance_source", "driver", "num_workers",
        "autoscale", "autotermination_minutes", "effective_spark_version", "policy_id",
        "executors", "state", "driver_healthy", "data_security_mode", "terminated_time",
        "spec", "spark_context_id", "custom_tags", "termination_reason", "last_restarted_time",
        "policy_info", "policy_name", "policy_definition", "policy_description",
        "max_clusters_per_user", "libraries", "created_at_timestamp", "is_default"
    ]

    # **Filter Columns That Exist in DataFrame**
    available_columns = [col_name for col_name in expected_columns if col_name in final_df.columns]

    # **Select Only Available Columns**
    cluster_policy_info_df = final_df.select(*available_columns)

    # **Display the Final DataFrame**
    display(cluster_policy_info_df)

else:
    print("⚠️ Running on AWS, skipping Azure-specific code.")

# COMMAND ----------

if cloud_str.upper() == "AWS":
    print("✅ Running in AWS Databricks. Executing API calls...")

    # **Databricks API Token**
    ACCESS_TOKEN = f"{ACCESS_TOKEN}"

    # **Read Clusters Table into DataFrame**
    clusters_df = spark.table(f"{SOURCE_CATALOG}.compute.clusters")

    # **Select Unique cluster_id & workspace_id**
    distinct_cluster_ids = clusters_df.select("cluster_id", "workspace_id").distinct()

    # **Join with Deployment Details**
    joined_df = distinct_cluster_ids.join(data_inventory_overview, on="workspace_id", how="inner")

    # **Convert to List for API Calls**
    cluster_list = joined_df.select("workspace_id", "deployment_url", "cluster_id").collect()

    # **Function to Fetch Cluster Info from API**
    def get_cluster_info(workspace_url, cluster_id):
        endpoint = f"{workspace_url}/api/2.1/clusters/get?cluster_id={cluster_id}"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        try:
            response = requests.get(endpoint, headers=headers)
            return response.json() if response.status_code == 200 else {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return {"error": str(e)}

    # **Function to Fetch Policy Info from API**
    def get_policy_info(workspace_url, policy_id):
        if not policy_id:
            return {
                "policy_name": None, "policy_definition": None, "policy_description": None,
                "max_clusters_per_user": None, "libraries": None, "creator_user_name": None,
                "created_at_timestamp": None, "is_default": None
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
                return {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return {"error": str(e)}

    # **Parallel API Calls for Cluster & Policy Info**
    cluster_results = []

    def process_cluster(row):
        workspace_id, deployment_url, cluster_id = row["workspace_id"], row["deployment_url"], row["cluster_id"]
        
        # **Get Cluster Info**
        cluster_info = get_cluster_info(deployment_url, cluster_id)
        policy_id = cluster_info.get("policy_id", None)  # Extract policy_id
        policy_info = get_policy_info(deployment_url, policy_id) if policy_id else {}

        # **Combine Cluster & Policy Info**
        return {
            "workspace_id": workspace_id,
            "deployment_url": deployment_url,
            "cluster_id": cluster_id,
            **cluster_info,  # Unpack cluster details
            **policy_info  # Unpack policy details
        }

    # **Use ThreadPoolExecutor for Parallel Processing**
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust based on cluster size
        future_to_cluster = {executor.submit(process_cluster, row): row for row in cluster_list}
        
        for future in as_completed(future_to_cluster):
            result = future.result()
            if result:
                cluster_results.append(result)

    # **Define Spark Schema**
    schema = StructType([
        StructField("workspace_id", StringType(), True),
        StructField("deployment_url", StringType(), True),
        StructField("cluster_id", StringType(), True),
        StructField("runtime_engine", StringType(), True),
        StructField("driver_node_type_id", StringType(), True),
        StructField("cluster_source", StringType(), True),
        StructField("last_state_loss_time", StringType(), True),
        StructField("creator_user_name", StringType(), True),
        StructField("node_type_id", StringType(), True),
        StructField("state_message", StringType(), True),
        StructField("cluster_cores", StringType(), True),
        StructField("instance_source", StringType(), True),
        StructField("last_activity_time", StringType(), True),
        StructField("single_user_name", StringType(), True),
        StructField("cluster_name", StringType(), True),
        StructField("cluster_memory_mb", StringType(), True),
        StructField("spark_version", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("policy_name", StringType(), True),
        StructField("policy_definition", StringType(), True),
        StructField("policy_description", StringType(), True),
        StructField("max_clusters_per_user", StringType(), True),
        StructField("libraries", StringType(), True),
        StructField("created_at_timestamp", StringType(), True),
        StructField("is_default", BooleanType(), True),
        StructField("state", StringType(), True),  # Convert state to string
        StructField("effective_spark_version", StringType(), True)  # Convert to String
    ])

    # **Convert to Spark DataFrame**
    cluster_policy_info_df = spark.createDataFrame(cluster_results, schema)

    # **Display the Resulting DataFrame**
    display(cluster_policy_info_df)
else:
    print("⚠️ Running on AZURE, skipping AWS-specific code.")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
cluster_policy_info_df.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.cluster_policy_assets")

print("Metadata successfully stored in `metadata.cluster_policy_assets`.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Models

# COMMAND ----------

def apply_aiexclusions(df, schema_col="schema_name", catalog_col="catalog_name"):
    return df.filter(
        (~col(schema_col).isin(EXCLUDED_SCHEMAS)) &
        (~col(catalog_col).isin(EXCLUDED_SHARES))  
    )

# COMMAND ----------

# **Retrieve All Workspace URLs from `data_inventory_overview`**
workspace_url_list = data_inventory_overview.select("deployment_url").distinct().collect()
workspace_urls = [row["deployment_url"] for row in workspace_url_list]

# **Authentication Token (Ensure it's Correct)**
access_token = f"{ACCESS_TOKEN}"

# **Headers for Authentication**
headers = {
    "Authorization": f"Bearer {access_token}"
}

# **Function to Fetch All Registered Models**
def fetch_all_models(workspace_url):
    api_url = f"{workspace_url}/api/2.1/unity-catalog/models"
    all_models = []
    next_page_token = None

    while True:
        params = {"max_results": 50}
        if next_page_token:
            params["page_token"] = next_page_token

        response = requests.get(api_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            all_models.extend(data.get("registered_models", []))
            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break
        else:
            print(f"❌ Error fetching models from {workspace_url}: {response.status_code} - {response.text}")
            break

    return all_models

# **Function to Fetch Model Versions**
def fetch_model_versions(workspace_url, full_model_name):
    api_url = f"{workspace_url}/api/2.1/unity-catalog/models/{full_model_name}/versions"
    all_versions = []
    next_page_token = None

    while True:
        params = {"max_results": 100, "include_aliases": True, "include_browse": True}
        if next_page_token:
            params["page_token"] = next_page_token

        response = requests.get(api_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            all_versions.extend(data.get("model_versions", []))
            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break
        else:
            print(f"❌ Error fetching versions for {full_model_name} in {workspace_url}: {response.status_code} - {response.text}")
            break

    return all_versions

# **Function to Fetch Full Model Version Details**
def fetch_model_version_details(workspace_url, full_model_name, version):
    api_url = f"{workspace_url}/api/2.1/unity-catalog/models/{full_model_name}/versions/{version}"
    
    params = {"include_browse": True, "include_aliases": True}
    response = requests.get(api_url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Error fetching details for {full_model_name} v{version} in {workspace_url}: {response.status_code} - {response.text}")
        return None

# **Schema for DataFrame**
model_version_schema = StructType([
    StructField("workspace_url", StringType(), True),  # ✅ Add workspace URL
    StructField("model_name", StringType(), True),
    StructField("catalog_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("source", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("run_workspace_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("version", IntegerType(), True),
    StructField("storage_location", StringType(), True),
    StructField("metastore_id", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("created_by", StringType(), True),
    StructField("updated_at", LongType(), True),
    StructField("updated_by", StringType(), True),
    StructField("id", StringType(), True),
    StructField("dependencies", ArrayType(MapType(StringType(), StringType())), True),
    StructField("aliases", ArrayType(MapType(StringType(), StringType())), True)
])

# **Fetch Data for All Workspaces**
all_detailed_model_versions = []

for workspace_url in workspace_urls:
    print(f"🚀 Fetching models from {workspace_url}...")

    models_data = fetch_all_models(workspace_url)
    model_full_names = [model["full_name"] for model in models_data]

    for full_model_name in model_full_names:
        model_versions = fetch_model_versions(workspace_url, full_model_name)

        for model_version in model_versions:
            version_number = model_version["version"]
            full_details = fetch_model_version_details(workspace_url, full_model_name, version_number)

            if full_details:
                full_details["workspace_url"] = workspace_url  # ✅ Add workspace URL
                all_detailed_model_versions.append(full_details)

# **Format Data for DataFrame**
formatted_data = []
for model_version in all_detailed_model_versions:
    dependencies = model_version.get("model_version_dependencies", {}).get("dependencies", [])

    # **Convert Dependencies to Array of Key-Value Pairs**
    extracted_dependencies = []
    for dep in dependencies:
        if "table" in dep:
            extracted_dependencies.append({"type": "table", "name": dep["table"]["table_full_name"]})
        elif "function" in dep:
            extracted_dependencies.append({"type": "function", "name": dep["function"]["function_full_name"]})

    aliases = model_version.get("aliases", [])
    extracted_aliases = [{"alias_name": alias["alias_name"], "version_num": alias["version_num"]} for alias in aliases]

    formatted_data.append({
        "workspace_url": model_version["workspace_url"],  # ✅ Add workspace URL
        "model_name": model_version["model_name"],
        "catalog_name": model_version["catalog_name"],
        "schema_name": model_version["schema_name"],
        "source": model_version.get("source", ""),
        "comment": model_version.get("comment", ""),
        "run_id": model_version.get("run_id", None),
        "run_workspace_id": model_version.get("run_workspace_id", None),
        "status": model_version["status"],
        "version": model_version["version"],
        "storage_location": model_version["storage_location"],
        "metastore_id": model_version["metastore_id"],
        "created_at": model_version["created_at"],
        "created_by": model_version["created_by"],
        "updated_at": model_version["updated_at"],
        "updated_by": model_version["updated_by"],
        "id": model_version["id"],
        "dependencies": extracted_dependencies,
        "aliases": extracted_aliases
    })

# **Create Spark DataFrame**
model_versions_df = spark.createDataFrame(formatted_data, model_version_schema)

filtered_aidf = apply_aiexclusions(model_versions_df)

# **Display DataFrame**
display(filtered_aidf)


# COMMAND ----------

# MAGIC %md
# MAGIC # Volumes

# COMMAND ----------

# Load the required system tables
volume_privileges_df = load_system_table("volume_privileges", "volume_privileges")
volume_tags_df = load_system_table("volume_tags", "volume_tags")
volumes_df = load_system_table("volumes", "volumes")

# Ensure required tables are loaded
if not all([volume_privileges_df, volume_tags_df, volumes_df]):
    print("❌ Critical volume tables are missing. Aborting process.")
    exit()

# Join volume_tags with volumes
volumes_df = volumes_df.join(
    broadcast(volume_tags_df), 
    (volumes_df["volumes_volume_name"] == volume_tags_df["volume_tags_volume_name"]) &
    (volumes_df["volumes_volume_schema"] == volume_tags_df["volume_tags_schema_name"]) &
    (volumes_df["volumes_volume_catalog"] == volume_tags_df["volume_tags_catalog_name"]),
    "left"
)

# Join volume_privileges with volumes
volumes_df = volumes_df.join(
    broadcast(volume_privileges_df), 
    (volumes_df["volumes_volume_name"] == volume_privileges_df["volume_privileges_volume_name"]) &
    (volumes_df["volumes_volume_schema"] == volume_privileges_df["volume_privileges_volume_schema"]) &
    (volumes_df["volumes_volume_catalog"] == volume_privileges_df["volume_privileges_volume_catalog"]),
    "left"
)

volumes_df = volumes_df.select("volumes_volume_catalog",
"volumes_volume_schema",
"volumes_volume_name",
"volumes_volume_type",
"volumes_volume_owner",
"volumes_comment",
"volumes_storage_location",
"volumes_created",
"volumes_created_by",
"volumes_last_altered",
"volumes_last_altered_by",
"volume_tags_tag_name",
"volume_tags_tag_value",
"volume_privileges_grantor",
"volume_privileges_grantee",
"volume_privileges_privilege_type",
"volume_privileges_is_grantable",
"volume_privileges_inherited_from")

# Display the joined DataFrame
display(volumes_df)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
volumes_df.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.volumes_assets")

print("Metadata successfully stored in `metadata.volumes_assets`.")
