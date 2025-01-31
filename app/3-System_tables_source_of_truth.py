# Databricks notebook source
# MAGIC %md
# MAGIC Only run in core account/workspace

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col
from pyspark.sql.types import MapType
from concurrent.futures import ThreadPoolExecutor, as_completed


# COMMAND ----------

dbutils.widgets.text("SOURCE_CATALOG", "system")
dbutils.widgets.text("OBJECTIVE_CATALOG", "cog_system")
dbutils.widgets.text("INFO_SCHEMA_TARGET", "cog_information_schema")
dbutils.widgets.text("EXCLUDED_SCHEMAS", "information_schema", "ai")
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

# MAGIC %md
# MAGIC # Unit Testing

# COMMAND ----------

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

# Filter record_counts_df by assistant_events table
filtered_df = record_counts_df.filter(record_counts_df.table_name == "assistant_events")

# Sum all the remaining counts except the ones that have cog_system in catalog
remaining_sum = filtered_df.filter(~filtered_df.catalog.contains("cog_system")).agg({"count": "sum"}).collect()[0][0]

# Get the count for cog_system
cog_system_count = filtered_df.filter(filtered_df.catalog.contains("cog_system")).agg({"count": "sum"}).collect()[0][0]

# Ensure the sum equals the count for cog_system
assert remaining_sum == cog_system_count, "The sum of remaining counts does not equal the count for cog_system"

remaining_sum, cog_system_count
