# Databricks notebook source
# Inspect the schema to find the correct column name
providers_df = spark.sql("SHOW PROVIDERS")

# Assuming the correct column name is 'provider'
providers = [row.name for row in providers_df.collect()]

shares = []
for provider in providers:
    shares_df = spark.sql(f"SHOW SHARES IN PROVIDER `{provider}`")
    shares.extend([row.name for row in shares_df.collect()])

# # Convert the list of shares to a DataFrame
shares = spark.createDataFrame([(share,) for share in shares], ["share_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Import libs

# COMMAND ----------

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

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.metadata")

# Save DataFrame as a Table for Future Use
metadata_df.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_CATALOG}.metadata.assets_metadata")

print("Metadata successfully stored in `metadata.assets_metadata`.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Asset Properties 

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
cluster_policy_info_df = final_df.select(
"workspace_id",
"deployment_url",
"cluster_id",
"runtime_engine",
"driver_node_type_id",
"cluster_source",
"last_state_loss_time",
"creator_user_name",
"node_type_id",
"state_message",
"cluster_cores",
"instance_source",
"last_activity_time",
"single_user_name",
"cluster_name",
"cluster_memory_mb",
"spark_version",
"spark_conf",
"default_tags",
"cluster_log_conf",
"start_time",
"jdbc_port",
"spark_env_vars",
"enable_local_disk_encryption",
"init_scripts_safe_mode",
"assigned_principal",
"enable_elastic_disk",
"disk_spec",
"init_scripts",
"azure_attributes",
"error",
"driver_instance_source",
"driver",
"num_workers",
"autoscale",
"autotermination_minutes",
"effective_spark_version",
"policy_id",
"executors",
"state",
"driver_healthy",
"data_security_mode",
"terminated_time",
"spec",
"spark_context_id",
"cluster_log_status",
"custom_tags",
"termination_reason",
"last_restarted_time",
"policy_info",
"policy_name",
"policy_definition",
"policy_description",
"max_clusters_per_user",
"libraries",
"created_at_timestamp",
"is_default"
 )

# Display the resulting DataFrame
display(cluster_policy_info_df)


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

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import time

# Define the schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("catalog_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("id", StringType(), True),
    StructField("metastore_id", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("created_by", StringType(), True),
    StructField("updated_at", LongType(), True),
    StructField("updated_by", StringType(), True),
    StructField("storage_location", StringType(), True),
    StructField("securable_type", StringType(), True),
    StructField("securable_kind", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("browse_only", BooleanType(), True)
])

# Function to fetch models with pagination
def fetch_all_models(api_url, headers, max_results=50):
    models_list = []
    next_page_token = None

    while True:
        params = {'max_results': max_results}
        if next_page_token:
            params['page_token'] = next_page_token

        response = requests.get(api_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            models_list.extend(data.get('registered_models', []))
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break
            # Introduce a delay to respect rate limits
            time.sleep(0.15)  # Adjust the sleep time as needed
        else:
            raise Exception(f"Error fetching models: {response.status_code} - {response.text}")

    return models_list

# TODO : Update the API endpoint and headers with extracted deployment_url
# API endpoint and headers
api_url = "https://adb-3288368185694203.3.azuredatabricks.net/api/2.1/unity-catalog/models"
headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

# Fetch all models
try:
    models_data = fetch_all_models(api_url, headers)
except Exception as e:
    print(f"Error fetching models: {e}")

# Create DataFrame
asset_models = spark.createDataFrame(models_data, schema)

# Show DataFrame
display(asset_models)


# COMMAND ----------

distinct_full_names = asset_models.select("full_name").distinct().collect()
distinct_full_names = [row["full_name"] for row in distinct_full_names]
distinct_full_names

# COMMAND ----------

# **Databricks API Configuration**
databricks_instance = "https://adb-3288368185694203.3.azuredatabricks.net"
access_token = f"{ACCESS_TOKEN}"

# **Headers for Authentication**
headers = {
    "Authorization": f"Bearer {access_token}"
}

# **Function to Fetch All Registered Models**
def fetch_all_models():
    api_url = f"{databricks_instance}/api/2.1/unity-catalog/models"
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
            print(f"❌ Error fetching models: {response.status_code} - {response.text}")
            break

    return all_models

# **Function to Fetch Model Versions**
def fetch_model_versions(full_model_name):
    api_url = f"{databricks_instance}/api/2.1/unity-catalog/models/{full_model_name}/versions"
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
            print(f"❌ Error fetching versions for {full_model_name}: {response.status_code} - {response.text}")
            break

    return all_versions

# **Function to Fetch Full Details of a Specific Model Version**
def fetch_model_version_details(full_model_name, version):
    api_url = f"{databricks_instance}/api/2.1/unity-catalog/models/{full_model_name}/versions/{version}"
    
    params = {"include_browse": True, "include_aliases": True}
    response = requests.get(api_url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Error fetching details for {full_model_name} v{version}: {response.status_code} - {response.text}")
        return None

# **Fetch All Models**
models_data = fetch_all_models()
model_full_names = [model["full_name"] for model in models_data]

# **Fetch All Versions & Their Full Details**
detailed_model_versions = []

for full_model_name in model_full_names:
    model_versions = fetch_model_versions(full_model_name)
    
    for model_version in model_versions:
        version_number = model_version["version"]
        full_details = fetch_model_version_details(full_model_name, version_number)
        
        if full_details:
            detailed_model_versions.append(full_details)

# **Define Schema for DataFrame**
model_version_schema = StructType([
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
    StructField("dependencies", ArrayType(MapType(StringType(), StringType())), True),  # Store as ARRAY of key-value pairs
    StructField("aliases", ArrayType(MapType(StringType(), StringType())), True)
])

# **Format Data for DataFrame**
formatted_data = []
for model_version in detailed_model_versions:
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
        "dependencies": extracted_dependencies,  # Store as Array of Maps
        "aliases": extracted_aliases
    })

# **Create Spark DataFrame**
model_versions_df = spark.createDataFrame(formatted_data, model_version_schema)

# **Display DataFrame**
display(model_versions_df)


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

# COMMAND ----------

# MAGIC %md
# MAGIC # dev testing zone do not run

# COMMAND ----------

import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# **Databricks API Configuration**
distinct_deployment_urls = data_inventory_overview.select("deployment_url").distinct().rdd.flatMap(lambda x: x).collect()

access_token = f"{ACCESS_TOKEN}"  # ✅ Ensure this is set correctly

# **Headers for Authentication**
headers = {
    "Authorization": f"Bearer {access_token}"
}

# **Step 1: Fetch All Available Catalogs**
def fetch_catalogs(databricks_instance):
    catalogs_api_url = f"{databricks_instance}/api/2.1/unity-catalog/catalogs"
    response = requests.get(catalogs_api_url, headers=headers)
    if response.status_code == 200:
        return [catalog["name"] for catalog in response.json().get("catalogs", [])]
    else:
        print(f"❌ Error fetching catalogs from {databricks_instance}: {response.status_code} - {response.text}")
        return []

# **Step 2: Fetch All Schemas for a Given Catalog**
def fetch_schemas(databricks_instance, catalog_name):
    schemas_api_url = f"{databricks_instance}/api/2.1/unity-catalog/schemas"
    params = {"catalog_name": catalog_name}
    response = requests.get(schemas_api_url, headers=headers, params=params)
    if response.status_code == 200:
        return [schema["name"] for schema in response.json().get("schemas", [])]
    else:
        print(f"❌ Error fetching schemas for {catalog_name} from {databricks_instance}: {response.status_code} - {response.text}")
        return []

# **Step 3: Fetch Volumes for a Given (Catalog, Schema)**
def fetch_volumes(databricks_instance, catalog_name, schema_name):
    volumes_api_url = f"{databricks_instance}/api/2.1/unity-catalog/volumes"
    volumes_list = []
    next_page_token = None

    while True:
        params = {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "max_results": 1000  # ✅ Fetch as many as possible
        }
        if next_page_token:
            params["page_token"] = next_page_token

        response = requests.get(volumes_api_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            volumes_list.extend(data.get("volumes", []))
            next_page_token = data.get("next_page_token")

            if not next_page_token:
                break  # ✅ Exit loop when no more pages
            time.sleep(0.15)  # ✅ Respect API rate limits
        elif response.status_code == 429:
            print(f"⚠️ Rate limit hit. Retrying after 5 seconds...")
            time.sleep(5)
        else:
            print(f"❌ Error fetching volumes for {catalog_name}.{schema_name} from {databricks_instance}: {response.status_code} - {response.text}")
            break

    return volumes_list

volume_results = []

for databricks_instance in distinct_deployment_urls:
    # **Fetch All Catalogs**
    catalogs = fetch_catalogs(databricks_instance)

    if not catalogs:
        print(f"⚠️ No catalogs found in {databricks_instance}.")
        continue

    print(f"✅ Found {len(catalogs)} catalogs in {databricks_instance}: {catalogs}")

    # **Fetch All Schemas for Each Catalog in Parallel**
    schema_tasks = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_catalog = {executor.submit(fetch_schemas, databricks_instance, catalog): catalog for catalog in catalogs}

        for future in as_completed(future_to_catalog):
            catalog_name = future_to_catalog[future]
            schemas = future.result()
            if schemas:
                schema_tasks[catalog_name] = schemas

    print(f"✅ Retrieved schemas for {len(schema_tasks)} catalogs in {databricks_instance}.")

    # **Fetch All Volumes in Parallel**
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_schema = {
            executor.submit(fetch_volumes, databricks_instance, catalog, schema): (catalog, schema)
            for catalog, schemas in schema_tasks.items()
            for schema in schemas
        }

        for future in as_completed(future_to_schema):
            catalog_name, schema_name = future_to_schema[future]
            volumes = future.result()
            if volumes:
                volume_results.extend(volumes)

# **Check API Response Before Processing**
if not volume_results:
    print("⚠️ No volumes found. Check API response and permissions.")
else:
    print(f"✅ Retrieved {len(volume_results)} volumes.")
    print("🔍 Sample Volume Data:")
    print(volume_results[0])  # ✅ Print first result to debug

# **Load Data into Spark DataFrame**
df = spark.createDataFrame(volume_results)

# **Display Everything for Debugging**
print("🔍 DataFrame Schema:")
df.printSchema()  # ✅ Show all detected fields
display(df)  # ✅ View all retrieved volumes

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

