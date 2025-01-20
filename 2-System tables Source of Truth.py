# Databricks notebook source
dbutils.widgets.text("SOURCE_CATALOG", "system")
dbutils.widgets.text("OBJECTIVE_CATALOG", "cog_system")
dbutils.widgets.text("INFO_SCHEMA_TARGET", "cog_information_schema")
dbutils.widgets.text("EXCLUDED_SCHEMAS", "ai")
dbutils.widgets.text("EXCLUDED_TABLES", "_sqldf,__internal_logging")

SOURCE_CATALOG = dbutils.widgets.get("SOURCE_CATALOG")
TARGET_CATALOG = dbutils.widgets.get("OBJECTIVE_CATALOG")
INFO_SCHEMA_TARGET = dbutils.widgets.get("INFO_SCHEMA_TARGET")
EXCLUDED_SCHEMAS = set(dbutils.widgets.get("EXCLUDED_SCHEMAS").split(","))
EXCLUDED_TABLES = set(dbutils.widgets.get("EXCLUDED_TABLES").split(","))

# COMMAND ----------

# MAGIC %md
# MAGIC # cog_system

# COMMAND ----------

try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print("Available catalogs:", [row.asDict() for row in catalogs])
except Exception as e:
    print(f"Error retrieving catalogs: {str(e)}")
    catalogs = []

# Filtering logic
def is_valid_catalog(catalog_name):
    # Check if the catalog name contains "system" and does not contain "coe"
    return "system" in catalog_name and "coe" not in catalog_name

# Apply the filter to get relevant catalogs
source_catalogs = [
    row['catalog'] for row in catalogs if is_valid_catalog(row['catalog'])
]
print(f"Filtered source catalogs: {source_catalogs}")

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col
from pyspark.sql.types import MapType
from pyspark.sql.utils import AnalysisException
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
# SOURCE_CATALOGS = ["e2_fe_system_tables", "vk_uc_system_tables", "system"]  # Shared source catalogs
target_catalog = f"{TARGET_CATALOG}"  # Centralized catalog for merged data
excluded_schemas = f"{EXCLUDED_SCHEMAS}"   # Schemas to exclude
excluded_tables = f"{EXCLUDED_TABLES}"  # Tables to exclude


def create_catalog_if_not_exists(catalog_name):
    """
    Creates a catalog if it does not already exist.
    """
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        print(f"Catalog '{catalog_name}' is ready.")
    except Exception as e:
        print(f"Error creating catalog '{catalog_name}': {str(e)}")


def fetch_schemas_and_tables():
    """
    Dynamically fetches schemas and tables from the source catalogs.
    """
    schemas_and_tables = {}

    for catalog in source_catalogs:
        try:
            # Fetch schemas in the catalog
            schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
            schema_names = [row['namespace' if 'namespace' in row.asDict() else 'databaseName'] for row in schemas]
            
            for schema in schema_names:
                if schema not in excluded_schemas:
                    # Fetch tables in the schema
                    try:
                        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
                        table_names = [row['tableName'] for row in tables if row['tableName'] not in excluded_tables]
                        
                        if table_names:
                            if schema not in schemas_and_tables:
                                schemas_and_tables[schema] = set()
                            schemas_and_tables[schema].update(table_names)
                    except AnalysisException:
                        print(f"Error accessing tables in schema '{schema}' of catalog '{catalog}'. Skipping...")
        except AnalysisException:
            print(f"Error accessing catalog '{catalog}'. Skipping...")

    # Convert sets back to lists for easier processing
    return {schema: list(tables) for schema, tables in schemas_and_tables.items()}


def generate_uuid_expression(df):
    """
    Generate a UUID expression by concatenating all scalar column values (excluding maps).
    """
    included_columns = [
        field.name for field in df.schema.fields
        if not isinstance(field.dataType, MapType)
    ]
    uuid_expr = sha2(
        concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in included_columns]),
        256
    )
    return uuid_expr


def merge_table(schema_name, table_name):
    """
    Merge data from multiple source catalogs into a centralized target table with deduplication.
    """
    target_table = f"{target_catalog}.{schema_name}.{table_name}"

    # Create the target schema if it doesn't exist
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name}")
    except Exception as e:
        print(f"Error creating schema '{schema_name}' in target catalog: {str(e)}")
        return

    # Initialize an empty DataFrame for merged data
    merged_df = None

    # Process each source catalog
    for source_catalog in source_catalogs:
        source_table = f"{source_catalog}.{schema_name}.{table_name}"
        try:
            # Read the source table
            source_df = spark.sql(f"SELECT * FROM {source_table}")

            # Add UUID for deduplication
            source_df = source_df.withColumn("uuid", generate_uuid_expression(source_df))

            if merged_df is None:
                merged_df = source_df
            else:
                merged_df = merged_df.unionByName(source_df, allowMissingColumns=True)

        except AnalysisException:
            print(f"Table {source_table} does not exist in catalog {source_catalog}. Skipping...")
        except Exception as e:
            print(f"Error processing table {source_table}: {str(e)}")

    if merged_df is not None:
        try:
            # Check if the target table exists
            table_exists = False
            try:
                spark.sql(f"DESCRIBE TABLE {target_table}")
                table_exists = True
            except AnalysisException:
                pass  # Table does not exist

            if table_exists:
                # Read the existing target table and add UUID
                target_df = spark.sql(f"SELECT * FROM {target_table}")
                target_df = target_df.withColumn("uuid", generate_uuid_expression(target_df))

                # Identify new records using anti-join
                new_records_df = merged_df.alias("source").join(
                    target_df.alias("target"),
                    on=col("source.uuid") == col("target.uuid"),
                    how="left_anti"
                ).drop("uuid")

                # Append new records if any
                new_records_count = new_records_df.count()
                if new_records_count > 0:
                    new_records_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
                    print(f"Appended {new_records_count} new records to '{target_table}'.")
                else:
                    print(f"No new records to append for '{target_table}'.")
            else:
                # Write deduplicated data as a new table
                merged_df.drop("uuid").write.format("delta").saveAsTable(target_table)
                print(f"Created table '{target_table}' with all records from source catalogs.")

        except Exception as e:
            print(f"Error writing merged data to {target_table}: {str(e)}")
    else:
        print(f"No data to merge for {target_table}.")


def process_schema_and_tables(schemas_and_tables):
    """
    Process all tables in all schemas dynamically.
    """
    for schema_name, table_names in schemas_and_tables.items():
        for table_name in table_names:
            merge_table(schema_name, table_name)


def main():
    """
    Main function to merge shared catalogs into a centralized catalog.
    """
    create_catalog_if_not_exists(target_catalog)

    # Dynamically fetch schemas and tables from source catalogs
    schemas_and_tables = fetch_schemas_and_tables()

    # Process schemas and tables in parallel
    futures = []
    with ThreadPoolExecutor() as executor:
        futures.append(executor.submit(process_schema_and_tables, schemas_and_tables))

        for future in as_completed(futures):
            future.result()

    print("\nMerging process completed.")


# Entry point
if __name__ == "__main__":
    main()


# COMMAND ----------

Error processing table fg_wp_aws_cog_system.access.table_lineage: (com.databricks.backend.daemon.data.client.DatabricksRemoteException) BAD_REQUEST: Databricks Default Storage cannot be accessed using Classic Compute. Please use Serverless compute to access data in Default Storage

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row

# Catalogs and target
# source_catalogs = ["e2_fe_system_tables", "vk_uc_system_tables", "system"] # run the previous cell
target_catalog = "cog_system"

# Specify the tables to merge (schema, table)
specified_tables = [
    ("access", "assistant_events"), ("access", "audit"), 
    ("billing", "list_prices"), ("compute","node_types"), 
    ("compute","warehouses"), ("compute","clusters"), ("query","history"),
    ("serving", "endpoint_usage")]

# Create the target catalog if it doesn't exist
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
    print(f"Catalog '{target_catalog}' is ready.")
except Exception as e:
    print(f"Error creating catalog '{target_catalog}': {str(e)}")

# Initialize a list to store record counts for validation
record_counts = []

# Process each specified table
for schema_name, table_name in specified_tables:
    target_table = f"{target_catalog}.{schema_name}.{table_name}"

    # Create the target schema if it doesn't exist
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name}")
    except Exception as e:
        print(f"Error creating schema '{schema_name}' in target catalog: {str(e)}")
        continue

    # Initialize an empty DataFrame for merging data
    merged_df = None

    # Merge data from all source catalogs
    for catalog_name in source_catalogs:
        source_table = f"{catalog_name}.{schema_name}.{table_name}"

        try:
            # Read the source table
            source_df = spark.sql(f"SELECT * FROM {source_table}")

            # Count records for validation
            count = source_df.count()
            record_counts.append(Row(catalog=catalog_name, schema=schema_name, table=table_name, count=count))

            print(f"Read {count} records from {source_table}.")

            # Merge the data
            if merged_df is None:
                merged_df = source_df
            else:
                merged_df = merged_df.unionByName(source_df, allowMissingColumns=True)

        except AnalysisException:
            print(f"Table {source_table} does not exist. Skipping...")
        except Exception as e:
            print(f"Error processing table {source_table}: {str(e)}")

    # Write the merged data to the target table
    if merged_df is not None:
        try:
            # print(f"Writing merged data to {target_table}...")
            # merged_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

            # Count records in the target table for validation
            merged_count = merged_df.count()
            record_counts.append(Row(catalog=target_catalog, schema=schema_name, table=table_name, count=merged_count))
            print(f"Merged table {target_table} has {merged_count} records.")
        except Exception as e:
            print(f"Error writing merged table {target_table}: {str(e)}")
    else:
        print(f"No data to merge for {target_table}.")

# Create a DataFrame from record counts and display
record_counts_df = spark.createDataFrame(record_counts)
display(record_counts_df)

# COMMAND ----------

print(2246629
+2945977261
+2430655
+105306149)

# COMMAND ----------

# MAGIC %md
# MAGIC # archive

# COMMAND ----------

# Catalogs
source_catalogs = ["system", "WB_system_tables", "WC_system_tables"]  # Catalogs to merge
target_catalog = "gov_system_tables"  # The resulting catalog

# Step 1: Create the target catalog
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
    print(f"Catalog '{target_catalog}' created successfully.")
except Exception as e:
    print(f"Error creating catalog '{target_catalog}': {str(e)}")

# Step 2: Iterate through all schemas and tables in the source catalogs
for catalog_name in source_catalogs:
    print(f"Processing catalog: {catalog_name}")

    # Get all schemas in the current catalog
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_names = [row['namespace'] for row in schemas]
    except AnalysisException as e:
        print(f"Error accessing schemas in catalog {catalog_name}: {str(e)}")
        continue

    # Iterate through each schema
    for schema_name in schema_names:
        print(f"Processing schema: {schema_name} in catalog {catalog_name}")

        # Create the schema in the target catalog if it doesn't exist
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name}")
            print(f"Schema '{target_catalog}.{schema_name}' created successfully.")
        except Exception as e:
            print(f"Error creating schema '{schema_name}' in target catalog: {str(e)}")
            continue

        # Get all tables in the current schema
        try:
            tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
            table_names = [row['tableName'] for row in tables]
        except AnalysisException as e:
            print(f"Error accessing tables in schema {schema_name} of catalog {catalog_name}: {str(e)}")
            continue

        # Iterate through each table in the schema
        for table_name in table_names:
            print(f"Processing table: {schema_name}.{table_name} in catalog {catalog_name}")
            target_table = f"{target_catalog}.{schema_name}.{table_name}"

            try:
                # Read the source table
                source_df = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")

                # Check if the target table exists
                table_exists = False
                try:
                    spark.sql(f"DESCRIBE TABLE {target_table}")
                    table_exists = True
                except AnalysisException:
                    table_exists = False

                if table_exists:
                    # Merge the source data into the target table
                    print(f"Merging data into {target_table}")
                    target_df = spark.table(target_table)

                    # Perform the merge operation
                    (
                        target_df.alias("t")
                        .merge(
                            source_df.alias("s"),
                            " AND ".join(
                                [f"t.{col} = s.{col}" for col in source_df.columns if col in target_df.columns]
                            )
                        )
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                else:
                    # Create the target table if it doesn't exist
                    print(f"Creating and writing data to {target_table}")
                    source_df.write.format("delta").saveAsTable(target_table)

                print(f"Table {target_table} processed successfully.")

            except Exception as e:
                print(f"Error processing table {schema_name}.{table_name} in catalog {catalog_name}: {str(e)}")

print("\nMerge process completed.")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS gov_system CASCADE

# COMMAND ----------

e2_field_system
gov_system_tables
system
vk_uc_system
gov_system

# COMMAND ----------

try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print("Available catalogs:", [row.asDict() for row in catalogs])
except Exception as e:
    print(f"Error retrieving catalogs: {str(e)}")
    catalogs = []

# Filtering logic
def is_valid_catalog(catalog_name):
    # Split the catalog name by "_"
    parts = catalog_name.split("_")
    if len(parts) >= 3 or catalog_name == "system":  # Ensure there are at least three parts
        right_part = "_".join(parts[-2:])  # Rejoin the last two parts (to avoid issues with additional underscores)
        return right_part[:13] == "system_tables" or catalog_name == "system"
    return False

# Apply the filter to get relevant catalogs
source_catalogs = [
    row['catalog'] for row in catalogs if is_valid_catalog(row['catalog'])
]
print(f"Filtered source catalogs: {source_catalogs}")


# COMMAND ----------

# Step 2: Define the target catalog
target_catalog = "gov_system_tables"

# Step 3: Create the target catalog
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
    print(f"Catalog '{target_catalog}' created successfully.")
except Exception as e:
    print(f"Error creating catalog '{target_catalog}': {str(e)}")

# Continue with merging logic using source_catalogs...
for catalog_name in source_catalogs:
    print(f"Processing catalog: {catalog_name}")

    # Get all schemas in the current catalog
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_names = [row['namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'] for row in schemas]
    except Exception as e:
        print(f"Error accessing schemas in catalog {catalog_name}: {str(e)}")
        continue

    # Iterate through each schema
    for schema_name in schema_names:
        print(f"Processing schema: {schema_name} in catalog {catalog_name}")

        # Create the schema in the target catalog
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name}")
            print(f"Schema '{target_catalog}.{schema_name}' created successfully.")
        except Exception as e:
            print(f"Error creating schema '{schema_name}' in target catalog: {str(e)}")
            continue

        # Get all tables in the current schema
        try:
            tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
            table_names = [row['tableName'] for row in tables]
        except Exception as e:
            print(f"Error accessing tables in schema {schema_name} of catalog {catalog_name}: {str(e)}")
            continue

        # Iterate through each table in the schema
        for table_name in table_names:
            print(f"Processing table: {schema_name}.{table_name} in catalog {catalog_name}")
            target_table = f"{target_catalog}.{schema_name}.{table_name}"

            try:
                # Read the source table
                source_df = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")

                # Check if the target table exists
                table_exists = False
                try:
                    spark.sql(f"DESCRIBE TABLE {target_table}")
                    table_exists = True
                except Exception:
                    table_exists = False

                if table_exists:
                    # Merge the source data into the target table
                    print(f"Merging data into {target_table}")
                    target_df = spark.table(target_table)
                    (
                        target_df.alias("t")
                        .merge(
                            source_df.alias("s"),
                            " AND ".join(
                                [f"t.{col} = s.{col}" for col in source_df.columns if col in target_df.columns]
                            )
                        )
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                else:
                    # Create the target table if it doesn't exist
                    print(f"Creating and writing data to {target_table}")
                    source_df.write.format("delta").saveAsTable(target_table)

                print(f"Table {target_table} processed successfully.")

            except Exception as e:
                print(f"Error processing table {schema_name}.{table_name} in catalog {catalog_name}: {str(e)}")

print("\nMerge process completed.")


# COMMAND ----------

# Filtering logic
def is_valid_catalog(catalog_name):
    # Split the catalog name by "_"
    parts = catalog_name.split("_")
    if len(parts) >= 3:  # Ensure there are at least three parts
        right_part = "_".join(parts[-2:])  # Rejoin the last two parts (to avoid issues with additional underscores)
        return right_part[:13] == "system_tables"
    return False

# Apply the filter to get relevant catalogs
valid_catalogs = [
    row['catalog'] for row in catalogs if is_valid_catalog(row['catalog'])
]
print(f"Filtered source catalogs: {source_catalogs}")

# Step 2: Define the target catalog
target_catalog = "gov_system_tables"

# Step 1: Count records before the merge
print("Counting records before merge...")
pre_merge_counts = {}
for catalog_name in valid_catalogs:
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_names = [row['namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'] for row in schemas]

        for schema_name in schema_names:
            try:
                tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
                table_names = [row['tableName'] for row in tables]

                for table_name in table_names:
                    table_full_name = f"{catalog_name}.{schema_name}.{table_name}"
                    try:
                        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_full_name}").collect()[0]['cnt']
                        pre_merge_counts[table_full_name] = count
                        print(f"Catalog: {catalog_name}, Schema: {schema_name}, Table: {table_name}, Count: {count}")
                    except Exception as e:
                        print(f"Error counting records in table {table_full_name}: {str(e)}")
            except Exception as e:
                print(f"Error accessing tables in schema {schema_name} of catalog {catalog_name}: {str(e)}")
    except Exception as e:
        print(f"Error accessing schemas in catalog {catalog_name}: {str(e)}")

# Step 2: Perform the merge (assuming the merge logic is already implemented)
print("Performing merge...")

# Step 3: Create the target catalog
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
    print(f"Catalog '{target_catalog}' created successfully.")
except Exception as e:
    print(f"Error creating catalog '{target_catalog}': {str(e)}")

# Continue with merging logic using source_catalogs...
for catalog_name in source_catalogs:
    print(f"Processing catalog: {catalog_name}")

    # Get all schemas in the current catalog
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_names = [row['namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'] for row in schemas]
    except Exception as e:
        print(f"Error accessing schemas in catalog {catalog_name}: {str(e)}")
        continue

    # Iterate through each schema
    for schema_name in schema_names:
        print(f"Processing schema: {schema_name} in catalog {catalog_name}")

        # Create the schema in the target catalog
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name}")
            print(f"Schema '{target_catalog}.{schema_name}' created successfully.")
        except Exception as e:
            print(f"Error creating schema '{schema_name}' in target catalog: {str(e)}")
            continue

        # Get all tables in the current schema
        try:
            tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
            table_names = [row['tableName'] for row in tables]
        except Exception as e:
            print(f"Error accessing tables in schema {schema_name} of catalog {catalog_name}: {str(e)}")
            continue

        # Iterate through each table in the schema
        for table_name in table_names:
            print(f"Processing table: {schema_name}.{table_name} in catalog {catalog_name}")
            target_table = f"{target_catalog}.{schema_name}.{table_name}"

            try:
                # Read the source table
                source_df = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")

                # Check if the target table exists
                table_exists = False
                try:
                    spark.sql(f"DESCRIBE TABLE {target_table}")
                    table_exists = True
                except Exception:
                    table_exists = False

                if table_exists:
                    # Merge the source data into the target table
                    print(f"Merging data into {target_table}")
                    target_df = spark.table(target_table)
                    (
                        target_df.alias("t")
                        .merge(
                            source_df.alias("s"),
                            " AND ".join(
                                [f"t.{col} = s.{col}" for col in source_df.columns if col in target_df.columns]
                            )
                        )
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                else:
                    # Create the target table if it doesn't exist
                    print(f"Creating and writing data to {target_table}")
                    source_df.write.format("delta").saveAsTable(target_table)

                print(f"Table {target_table} processed successfully.")

            except Exception as e:
                print(f"Error processing table {schema_name}.{table_name} in catalog {catalog_name}: {str(e)}")

print("\nMerge process completed.")

# Step 3: Count records after the merge
print("Counting records after merge...")
post_merge_counts = {}
try:
    schemas = spark.sql(f"SHOW SCHEMAS IN {target_catalog}").collect()
    schema_names = [row['namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'] for row in schemas]

    for schema_name in schema_names:
        try:
            tables = spark.sql(f"SHOW TABLES IN {target_catalog}.{schema_name}").collect()
            table_names = [row['tableName'] for row in tables]

            for table_name in table_names:
                table_full_name = f"{target_catalog}.{schema_name}.{table_name}"
                try:
                    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_full_name}").collect()[0]['cnt']
                    post_merge_counts[table_full_name] = count
                    print(f"Target Catalog: {target_catalog}, Schema: {schema_name}, Table: {table_name}, Count: {count}")
                except Exception as e:
                    print(f"Error counting records in table {table_full_name}: {str(e)}")
        except Exception as e:
            print(f"Error accessing tables in schema {schema_name} of catalog {target_catalog}: {str(e)}")
except Exception as e:
    print(f"Error accessing schemas in target catalog {target_catalog}: {str(e)}")

# Step 4: Validation
print("\nValidation Results:")
for source_table, source_count in pre_merge_counts.items():
    table_parts = source_table.split(".")
    target_table = f"{target_catalog}.{table_parts[1]}.{table_parts[2]}"  # Assuming schema and table names are preserved
    target_count = post_merge_counts.get(target_table, 0)
    print(f"Source Table: {source_table}, Source Count: {source_count}, Merged Count: {target_count}")
    if source_count != target_count:
        print(f"⚠️ Validation mismatch for {source_table} -> {target_table}: {source_count} != {target_count}")
    else:
        print(f"✅ Validation passed for {source_table} -> {target_table}")


# COMMAND ----------

from pyspark.sql import Row

# Filtering logic
def is_valid_catalog(catalog_name):
    # Split the catalog name by "_"
    parts = catalog_name.split("_")
    if len(parts) >= 3:  # Ensure there are at least three parts
        right_part = "_".join(parts[-2:])  # Rejoin the last two parts (to avoid issues with additional underscores)
        return right_part[:13] == "system_tables"
    return False

# Apply the filter to get relevant catalogs
valid_catalogs = [
    row['catalog'] for row in catalogs if is_valid_catalog(row['catalog'])
]
print(f"Filtered source catalogs: {source_catalogs}")

# Step 2: Define the target catalog
target_catalog = "gov_system_tables"

# Initialize a list to store count results
record_counts = []

# Step 1: Create the target catalog
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
    print(f"Catalog '{target_catalog}' created successfully.")
except Exception as e:
    print(f"Error creating catalog '{target_catalog}': {str(e)}")

# Step 2: Count records for source catalogs
print("Counting records for source catalogs...")
for catalog_name in valid_catalogs:
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_names = [row['namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'] for row in schemas]

        for schema_name in schema_names:
            tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
            table_names = [row['tableName'] for row in tables]

            for table_name in table_names:
                table_full_name = f"{catalog_name}.{schema_name}.{table_name}"
                try:
                    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_full_name}").collect()[0]['cnt']
                    record_counts.append(Row(catalog=catalog_name, schema=schema_name, table=table_name, count=count))
                except Exception as e:
                    print(f"Error counting records in table {table_full_name}: {str(e)}")
    except Exception as e:
        print(f"Error accessing schemas in catalog {catalog_name}: {str(e)}")

# Step 3: Perform the merge (reuse your merge logic here)
print("Performing merge...")

# Step 3: Create the target catalog
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
    print(f"Catalog '{target_catalog}' created successfully.")
except Exception as e:
    print(f"Error creating catalog '{target_catalog}': {str(e)}")

# Continue with merging logic using source_catalogs...
for catalog_name in source_catalogs:
    print(f"Processing catalog: {catalog_name}")

    # Get all schemas in the current catalog
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_names = [row['namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'] for row in schemas]
    except Exception as e:
        print(f"Error accessing schemas in catalog {catalog_name}: {str(e)}")
        continue

    # Iterate through each schema
    for schema_name in schema_names:
        print(f"Processing schema: {schema_name} in catalog {catalog_name}")

        # Create the schema in the target catalog
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema_name}")
            print(f"Schema '{target_catalog}.{schema_name}' created successfully.")
        except Exception as e:
            print(f"Error creating schema '{schema_name}' in target catalog: {str(e)}")
            continue

        # Get all tables in the current schema
        try:
            tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
            table_names = [row['tableName'] for row in tables]
        except Exception as e:
            print(f"Error accessing tables in schema {schema_name} of catalog {catalog_name}: {str(e)}")
            continue

        # Iterate through each table in the schema
        for table_name in table_names:
            print(f"Processing table: {schema_name}.{table_name} in catalog {catalog_name}")
            target_table = f"{target_catalog}.{schema_name}.{table_name}"

            try:
                # Read the source table
                source_df = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")

                # Check if the target table exists
                table_exists = False
                try:
                    spark.sql(f"DESCRIBE TABLE {target_table}")
                    table_exists = True
                except Exception:
                    table_exists = False

                if table_exists:
                    # Merge the source data into the target table
                    print(f"Merging data into {target_table}")
                    target_df = spark.table(target_table)
                    (
                        target_df.alias("t")
                        .merge(
                            source_df.alias("s"),
                            " AND ".join(
                                [f"t.{col} = s.{col}" for col in source_df.columns if col in target_df.columns]
                            )
                        )
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                else:
                    # Create the target table if it doesn't exist
                    print(f"Creating and writing data to {target_table}")
                    source_df.write.format("delta").saveAsTable(target_table)

                print(f"Table {target_table} processed successfully.")

            except Exception as e:
                print(f"Error processing table {schema_name}.{table_name} in catalog {catalog_name}: {str(e)}")

print("\nMerge process completed.")

# Step 4: Count records for the target catalog
print("Counting records for the target catalog...")
try:
    schemas = spark.sql(f"SHOW SCHEMAS IN {target_catalog}").collect()
    schema_names = [row['namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'] for row in schemas]

    for schema_name in schema_names:
        tables = spark.sql(f"SHOW TABLES IN {target_catalog}.{schema_name}").collect()
        table_names = [row['tableName'] for row in tables]

        for table_name in table_names:
            table_full_name = f"{target_catalog}.{schema_name}.{table_name}"
            try:
                count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_full_name}").collect()[0]['cnt']
                record_counts.append(Row(catalog=target_catalog, schema=schema_name, table=table_name, count=count))
            except Exception as e:
                print(f"Error counting records in table {table_full_name}: {str(e)}")
except Exception as e:
    print(f"Error accessing schemas in target catalog {target_catalog}: {str(e)}")

# Step 5: Create a DataFrame and display the results
record_counts_df = spark.createDataFrame(record_counts)
display(record_counts_df)


# COMMAND ----------

record_counts_df_filtered = record_counts_df.filter(record_counts_df.table == "assistant_events")
display(record_counts_df_filtered)

# COMMAND ----------

#champion
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col
from pyspark.sql.types import MapType
from pyspark.sql.utils import AnalysisException
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
# SOURCE_CATALOGS = ["e2_fe_system_tables", "vk_uc_system_tables", "system"]  # Shared source catalogs
TARGET_CATALOG = "coe_system"  # Centralized catalog for merged data
EXCLUDED_SCHEMAS = {"information_schema", "ai"}  # Schemas to exclude
EXCLUDED_TABLES = {"_sqldf", "__internal_logging"}  # Tables to exclude


def create_catalog_if_not_exists(catalog_name):
    """
    Creates a catalog if it does not already exist.
    """
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        print(f"Catalog '{catalog_name}' is ready.")
    except Exception as e:
        print(f"Error creating catalog '{catalog_name}': {str(e)}")


def generate_uuid_expression(df):
    """
    Generate a UUID expression by concatenating all scalar column values (excluding maps).
    """
    included_columns = [
        field.name for field in df.schema.fields
        if not isinstance(field.dataType, MapType)
    ]
    uuid_expr = sha2(
        concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in included_columns]),
        256
    )
    return uuid_expr


def merge_table(schema_name, table_name):
    """
    Merge data from multiple source catalogs into a centralized target table with deduplication.
    """
    target_table = f"{TARGET_CATALOG}.{schema_name}.{table_name}"

    # Create the target schema if it doesn't exist
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{schema_name}")
    except Exception as e:
        print(f"Error creating schema '{schema_name}' in target catalog: {str(e)}")
        return

    # Initialize an empty DataFrame for merged data
    merged_df = None

    # Process each source catalog
    for source_catalog in source_catalogs:
        source_table = f"{source_catalog}.{schema_name}.{table_name}"
        try:
            # Read the source table
            source_df = spark.sql(f"SELECT * FROM {source_table}")

            # Add UUID for deduplication
            source_df = source_df.withColumn("uuid", generate_uuid_expression(source_df))

            if merged_df is None:
                merged_df = source_df
            else:
                merged_df = merged_df.unionByName(source_df, allowMissingColumns=True)

        except AnalysisException:
            print(f"Table {source_table} does not exist in catalog {source_catalog}. Skipping...")
        except Exception as e:
            print(f"Error processing table {source_table}: {str(e)}")

    if merged_df is not None:
        try:
            # Check if the target table exists
            table_exists = False
            try:
                spark.sql(f"DESCRIBE TABLE {target_table}")
                table_exists = True
            except AnalysisException:
                pass  # Table does not exist

            if table_exists:
                # Read the existing target table and add UUID
                target_df = spark.sql(f"SELECT * FROM {target_table}")
                target_df = target_df.withColumn("uuid", generate_uuid_expression(target_df))

                # Identify new records using anti-join
                new_records_df = merged_df.alias("source").join(
                    target_df.alias("target"),
                    on=col("source.uuid") == col("target.uuid"),
                    how="left_anti"
                ).drop("uuid")

                # Append new records if any
                new_records_count = new_records_df.count()
                if new_records_count > 0:
                    new_records_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
                    print(f"Appended {new_records_count} new records to '{target_table}'.")
                else:
                    print(f"No new records to append for '{target_table}'.")
            else:
                # Write deduplicated data as a new table
                merged_df.drop("uuid").write.format("delta").saveAsTable(target_table)
                print(f"Created table '{target_table}' with all records from source catalogs.")

        except Exception as e:
            print(f"Error writing merged data to {target_table}: {str(e)}")
    else:
        print(f"No data to merge for {target_table}.")


def process_schema_and_tables(schema_name, tables):
    """
    Process all specified tables in a schema.
    """
    for table_name in tables:
        merge_table(schema_name, table_name)


def main():
    """
    Main function to merge shared catalogs into a centralized catalog.
    """
    create_catalog_if_not_exists(TARGET_CATALOG)

    # Specify schemas and tables to merge
    schemas_and_tables = {
        "access": ["assistant_events", "audit"],
        "logs": ["system_logs"]
    }

    # Process each schema and its tables in parallel
    futures = []
    with ThreadPoolExecutor() as executor:
        for schema_name, tables in schemas_and_tables.items():
            futures.append(executor.submit(process_schema_and_tables, schema_name, tables))

        for future in as_completed(futures):
            future.result()

    print("\nMerging process completed.")


# Entry point
if __name__ == "__main__":
    main()

