# Databricks notebook source
# DBTITLE 1,he
import requests

# Replace these with your actual values
ACCOUNT_ID = "ccb842e7-2376-4152-b0b0-29fa952379b8"  # Example: "83988469-aba5-4d3d-bcad-f8707acc74cd"
CLIENT_ID = "83988469-aba5-4d3d-bcad-f8707acc74cd"
CLIENT_SECRET = "dose1f2a6b5faf019bee3eb6e0514794d83b"

# Azure OAuth Token Endpoint
token_url = f"https://accounts.azuredatabricks.net/oidc/accounts/{ACCOUNT_ID}/v1/token"
# AWS OAuth Token Endpoint
# token_url = f"https://accounts.cloud.databricks.com/oidc/accounts/{ACCOUNT_ID}/v1/token"

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
    print("Access Token:", ACCESS_TOKEN)
else:
    print(f"Error obtaining access token: {response.status_code} - {response.text}")

# COMMAND ----------

# Replace these with your actual values
ACCOUNT_ID = "ccb842e7-2376-4152-b0b0-29fa952379b8"

# Azure API endpoint to fetch all workspaces
api_endpoint = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{ACCOUNT_ID}/workspaces"
# AWS
# api_endpoint = f"https://accounts.cloud.databricks.com/api/2.0/accounts/{ACCOUNT_ID}/workspaces"

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

# Data Inventory Overview
# List of Databricks accounts mapped to the applications
# Account region (US-East2) hardcoded for now 
# Cloud provider (AWS/Azure)
query = """
SELECT DISTINCT 
    account_id, 
    workspace_id, 
    cloud
FROM system.billing.usage
"""
data_inventory_df = spark.sql(query)
display(data_inventory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC   CASE 
# MAGIC         WHEN account_id = 'e6e8162c-a42f-43a0-af86-312058795a14' THEN 'US-East2' 
# MAGIC         ELSE cloud 
# MAGIC     END AS region

# COMMAND ----------

data_inventory_overview = workspaces_data.join(data_inventory_df, on=["account_id", "workspace_id"], how="full")
display(data_inventory_overview)

# COMMAND ----------

from pyspark.sql.functions import concat, lit

data_inventory_overview = data_inventory_overview.withColumn(
    "deployment_url",
    concat(lit("https://"), data_inventory_overview["deployment_name"], lit(".azuredatabricks.net"))
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

total_users = users_df.select("user_id").distinct().count()
print(f"Total distinct users: {total_users}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Breakdown of user types (e.g., System Admin, Analyst, Privacy) - Get token for account level auth testing

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


