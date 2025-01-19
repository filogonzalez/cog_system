# Databricks notebook source
# Data Inventory Overview
    # List of Databricks accounts mapped to the applications
    # Cloud provider (AWS/Azure)
query = """
SELECT DISTINCT 
    account_id, 
    workspace_id, 
    cloud
FROM coe_system.billing.usage
"""
data_inventory_df = spark.sql(query)
display(data_inventory_df)

# COMMAND ----------

from pyspark.sql.functions import collect_set

# Load the data from the table
df = spark.table("system.access.audit")

# Get distinct account_id
distinct_account_ids = df.select("account_id").distinct()

# Collect the distinct account_ids into a list
account_ids_list = distinct_account_ids.agg(collect_set("account_id")).first()[0]

# Convert the list to a string
account_id_str = ','.join(account_ids_list)

# Load the data from the table
df_billing_usage = spark.table("system.billing.usage")

# Get distinct cloud values
distinct_clouds = df_billing_usage.select("cloud").distinct()

# Collect the distinct cloud values into a list
clouds_list = distinct_clouds.agg(collect_set("cloud")).first()[0]

# Convert the list to a string
cloud_str = ','.join(clouds_list)

# COMMAND ----------

# DBTITLE 1,he
import requests

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
    "lifetime_seconds": 3600,  # Token expires in 1 hour
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
    workspaces_data = spark.createDataFrame(response_json)
    display(workspaces_data)
else:
    print(f"Error fetching workspaces: {response.status_code} - {response.text}")

# COMMAND ----------

from pyspark.sql.functions import concat, lit, when

data_inventory_overview = workspaces_data.join(data_inventory_df, on=["account_id", "workspace_id"], how="full")

data_inventory_overview = data_inventory_overview.withColumn(
    "deployment_url",
    when(
        data_inventory_overview["cloud"] == "azure",
        concat(lit("https://"), data_inventory_overview["deployment_name"], lit(".azuredatabricks.net"))
    ).when(
        data_inventory_overview["cloud"] == "AWS",
        concat(lit("https://"), data_inventory_overview["deployment_name"], lit(".cloud.databricks.com"))
    )
)
display(data_inventory_overview)

# COMMAND ----------

# Breakdown of user types 
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

# Asset Name, Owner, Description, comments and tags
from pyspark.sql.utils import AnalysisException

# SQL Query for Efficient Metadata Extraction
metadata_query = """
WITH CatalogMetadata AS (
    SELECT 
        c.catalog_name, 
        c.owner AS catalog_owner, 
        ct.tag_value AS catalog_tags
    FROM system.information_schema.catalogs c
    LEFT JOIN system.information_schema.catalog_tags ct 
        ON c.catalog_name = ct.catalog_name
),

SchemaMetadata AS (
    SELECT 
        s.schema_name, 
        s.catalog_name, 
        s.owner AS schema_owner, 
        st.tag_value AS schema_tags
    FROM system.information_schema.schemata s
    LEFT JOIN system.information_schema.schema_tags st 
        ON s.schema_name = st.schema_name
),

TableMetadata AS (
    SELECT 
        t.table_name, 
        t.schema_name, 
        t.catalog_name, 
        t.owner AS table_owner, 
        t.comment AS table_comment, 
        tt.tag_value AS table_tags
    FROM system.information_schema.tables t
    LEFT JOIN system.information_schema.table_tags tt 
        ON t.table_name = tt.table_name
),

ColumnMetadata AS (
    SELECT 
        col.column_name, 
        col.table_name, 
        col.schema_name, 
        col.catalog_name, 
        col.comment AS column_comment, 
        ctt.tag_value AS column_tags
    FROM system.information_schema.columns col
    LEFT JOIN system.information_schema.catalog_tags ctt 
        ON col.column_name = ctt.column_name
)

SELECT 
    cm.catalog_name,
    cm.catalog_owner,
    cm.catalog_tags,
    sm.schema_name,
    sm.schema_owner,
    sm.schema_tags,
    tm.table_name,
    tm.table_owner,
    tm.table_comment,
    tm.table_tags,
    colm.column_name,
    colm.column_comment,
    colm.column_tags
FROM ColumnMetadata colm
LEFT JOIN TableMetadata tm ON colm.table_name = tm.table_name
LEFT JOIN SchemaMetadata sm ON tm.schema_name = sm.schema_name
LEFT JOIN CatalogMetadata cm ON sm.catalog_name = cm.catalog_name
"""

# Execute Query & Convert to DataFrame
try:
    metadata_df = spark.sql(metadata_query)
    print("✅ Successfully retrieved metadata!")
except AnalysisException as e:
    print(f"❌ Error retrieving metadata: {str(e)}")
    exit()

# Display Results
display(metadata_df)

# Save DataFrame as a Table for Future Use
# metadata_df.write.format("delta").mode("overwrite").saveAsTable("metadata.asset_details")

# print("✅ Metadata successfully stored in `metadata.asset_details`.")


# COMMAND ----------

from pyspark.sql.functions import col, broadcast
from pyspark.sql.utils import AnalysisException

# Function to safely load system tables
def load_system_table(table_name):
    """Safely loads a system information schema table."""
    try:
        df = spark.sql(f"SELECT * FROM system.information_schema.{table_name}")
        print(f"✅ Loaded {table_name}")
        df.printSchema()  # Debugging: Print schema
        return df
    except AnalysisException:
        print(f"❌ Table system.information_schema.{table_name} not found. Skipping...")
        return None

# Load metadata tables
catalogs_df = load_system_table("catalogs")
catalog_tags_df = load_system_table("catalog_tags")
schemata_df = load_system_table("schemata")
schema_tags_df = load_system_table("schema_tags")
tables_df = load_system_table("tables")
table_tags_df = load_system_table("table_tags")
columns_df = load_system_table("columns")
column_tags_df = load_system_table("column_tags")

# Ensure required tables are loaded
if not all([catalogs_df, schemata_df, tables_df, columns_df]):
    print("❌ Critical metadata tables are missing. Aborting process.")
    exit()

if "table_catalog" in columns_df.columns:
    columns_df = columns_df.withColumnRenamed("table_catalog", "catalog_name")

if "table_catalog" in tables_df.columns:
    tables_df = tables_df.withColumnRenamed("table_catalog", "catalog_name")

if "table_catalog" in schemata_df.columns:
    schemata_df = schemata_df.withColumnRenamed("table_catalog", "catalog_name")


# Join columns with column_tags
if column_tags_df:
    columns_df = columns_df.join(broadcast(column_tags_df), ["column_name"], "left")

# Join tables with table_tags
if table_tags_df:
    tables_df = tables_df.join(broadcast(table_tags_df), ["table_name"], "left")

# Join columns with tables
metadata_df = columns_df.join(tables_df, ["table_name", "schema_name", "catalog_name"], "left")

# Join schemata with schema_tags
if schema_tags_df:
    schemata_df = schemata_df.join(broadcast(schema_tags_df), ["schema_name"], "left")

# Join metadata with schemata
metadata_df = metadata_df.join(schemata_df, ["schema_name", "catalog_name"], "left")

# Join catalogs with catalog_tags
if catalog_tags_df:
    catalogs_df = catalogs_df.join(broadcast(catalog_tags_df), ["catalog_name"], "left")

# Join metadata with catalogs
metadata_df = metadata_df.join(catalogs_df, ["catalog_name"], "left")

# Drop duplicates (if needed)
metadata_df = metadata_df.dropDuplicates()

# Display results
display(metadata_df)


# COMMAND ----------

# Assets name, Assets owner, Assets description/comments Data Stewards, Tags (domain/category, confidentiality, sensitive data types, PII/PCI, business glossary association …)
from pyspark.sql.functions import col, broadcast
from pyspark.sql.utils import AnalysisException

# Function to safely load system tables with prefixed column names
def load_system_table(table_name, prefix):
    """Safely loads a system information schema table and adds a prefix to all columns."""
    try:
        df = spark.sql(f"SELECT * FROM system.information_schema.{table_name}")
        print(f"✅ Loaded {table_name}")
        # Add prefix to columns
        df = df.select([col(c).alias(f"{prefix}_{c}") for c in df.columns])
        df.printSchema()
        return df
    except AnalysisException:
        print(f"❌ Table system.information_schema.{table_name} not found. Skipping...")
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

# Ensure required tables are loaded
if not all([catalogs_df, schemata_df, tables_df, columns_df]):
    print("❌ Critical metadata tables are missing. Aborting process.")
    exit()

# 🟢 Fix: Join column_tags with columns using correct alias (`columns_column_name`)
if column_tags_df:
    columns_df = columns_df.join(
        broadcast(column_tags_df), columns_df["columns_column_name"] == column_tags_df["column_tags_column_name"], "left"
    )

# 🟢 Join table_tags with tables
if table_tags_df:
    tables_df = tables_df.join(
        broadcast(table_tags_df), tables_df["tables_table_name"] == table_tags_df["table_tags_table_name"], "left"
    )

# 🟢 Join columns with tables
metadata_df = columns_df.join(
    tables_df,
    (columns_df["columns_table_name"] == tables_df["tables_table_name"]) &
    (columns_df["columns_table_schema"] == tables_df["tables_table_schema"]) &
    (columns_df["columns_table_catalog"] == tables_df["tables_table_catalog"]),
    "left"
)
# 🟢 Join schema_tags with schemata
if schema_tags_df:
    schemata_df = schemata_df.join(
        broadcast(schema_tags_df), schemata_df["schemata_schema_name"] == schema_tags_df["schema_tags_schema_name"], "left"
    )

# 🟢 Join metadata with schemata
metadata_df = metadata_df.join(
    schemata_df,
    (metadata_df["columns_table_schema"] == schemata_df["schemata_schema_name"]) &
    (metadata_df["columns_table_catalog"] == schemata_df["schemata_catalog_name"]),
    "left"
)

# 🟢 Join catalog_tags with catalogs
if catalog_tags_df:
    catalogs_df = catalogs_df.join(
        broadcast(catalog_tags_df), catalogs_df["catalogs_catalog_name"] == catalog_tags_df["catalog_tags_catalog_name"], "left"
    )

# 🟢 Join metadata with catalogs
metadata_df = metadata_df.join(
    catalogs_df,
    metadata_df["columns_table_catalog"] == catalogs_df["catalogs_catalog_name"],
    "left"
).drop("columns_table_catalog", "columns_table_schema", "columns_table_catalog", "tables_table_schema", "tables_table_catalog", "schema_tags_catalog_name", "schema_tags_schema_name", "schemata_catalog_name", "columns_table_catalog", "catalog_tags_catalog_name", "table_tags_catalog_name", "table_tags_schema_name", "table_tags_table_name", "column_tags_catalog_name", "column_tags_schema_name", "column_tags_table_name", "column_tags_column_name")

# ✅ Drop duplicates (if needed)
metadata_df = metadata_df.dropDuplicates()

# ✅ Final Column Selection (Correct Order)
metadata_df = metadata_df.select(
    # Catalogs First
    col("catalogs_catalog_name").alias("catalog_name"),
    "catalogs_catalog_owner", "catalogs_comment", "catalogs_created", 
    "catalogs_created_by", "catalogs_last_altered", "catalogs_last_altered_by", 
    "catalog_tags_tag_name", "catalog_tags_tag_value",
    
    # Schemas Second
    col("schemata_schema_name").alias("schema_name"),
    "schemata_schema_owner", "schemata_comment", "schemata_created", 
    "schemata_created_by", "schemata_last_altered", "schemata_last_altered_by",
    "schema_tags_tag_name", "schema_tags_tag_value",
    
    # Tables Third
    col("tables_table_name").alias("table_name"),
    "tables_table_owner", "tables_table_type", "tables_comment", "tables_created",
    "tables_created_by", "tables_last_altered", "tables_last_altered_by",
    "table_tags_tag_name", "table_tags_tag_value",
    
    # Columns Last
    col("columns_column_name").alias("column_name"),
    "columns_data_type", "columns_comment", "columns_is_nullable", 
    "columns_character_maximum_length", "columns_numeric_precision", 
    "columns_numeric_scale", "columns_datetime_precision",
    "column_tags_tag_name", "column_tags_tag_value"
)

# ✅ Display results
display(metadata_df)


# COMMAND ----------

# Data Stewards, Tags (domain/category, confidentiality, sensitive data types, PII/PCI, business glossary association …)

df_column_tags = spark.table("system.information_schema.column_tags")
display(df_column_tags)

# COMMAND ----------

# size of asssets (in GB/MB tota)

# COMMAND ----------

# MAGIC %md
# MAGIC # Breakdown of user types (e.g., System Admin, Analyst, Privacy) - Get token for account level auth testing

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


# COMMAND ----------

# MAGIC %md
# MAGIC # Catalog details

# COMMAND ----------


