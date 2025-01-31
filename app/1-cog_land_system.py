# Databricks notebook source
# Import required libs
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col
from pyspark.sql.types import MapType
from pyspark.sql.utils import AnalysisException
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Passing parameters
SOURCE_CATALOG = f"{SOURCE_CATALOG}"  # Source catalog
TARGET_CATALOG = f"{TARGET_CATALOG}"  # Target catalog for Delta tables
INFO_SCHEMA_TARGET = f"{INFO_SCHEMA_TARGET}"  # Special schema for information_schema tables
EXCLUDED_SCHEMAS = f"{EXCLUDED_SCHEMAS}"  # Exclude system schema from direct overwrite
EXCLUDED_TABLES = f"{EXCLUDED_TABLES}"  # Other exclusions

def create_catalog_if_not_exists(catalog_name):
    """Creates a catalog if it does not already exist."""
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        print(f"✅ Catalog '{catalog_name}' is ready.")
    except Exception as e:
        print(f"❌ Error creating catalog '{catalog_name}': {str(e)}")

def generate_uuid_expression(df):
    """Generate a UUID expression by concatenating all scalar column values (excluding maps)."""
    included_columns = [field.name for field in df.schema.fields if not isinstance(field.dataType, MapType)]
    return sha2(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in included_columns]), 256)

def process_table(schema_name, table_name):
    """Processes a single table for incremental ingestion with UUID-based deduplication."""
    
    # Determine the correct schema for writing
    target_schema = INFO_SCHEMA_TARGET if schema_name == "information_schema" else schema_name
    
    target_table_name = f"{TARGET_CATALOG}.{target_schema}.{table_name}"
    source_table_name = f"{SOURCE_CATALOG}.{schema_name}.{table_name}"

    try:
        # Check if the target table exists
        table_exists = False
        try:
            spark.sql(f"DESCRIBE TABLE {target_table_name}")
            table_exists = True
        except AnalysisException:
            pass  # Table does not exist

        # Read the source table
        source_df = spark.sql(f"SELECT * FROM {source_table_name}")

        if table_exists:
            # Read the target table
            target_df = spark.sql(f"SELECT * FROM {target_table_name}")

            # Generate UUIDs for deduplication
            source_df = source_df.withColumn("source_uuid", generate_uuid_expression(source_df))
            target_df = target_df.withColumn("target_uuid", generate_uuid_expression(target_df))

            # Identify new records using left anti-join
            new_records_df = source_df.alias("source").join(
                target_df.alias("target"),
                on=col("source.source_uuid") == col("target.target_uuid"),
                how="left_anti"
            ).drop("source_uuid", "target_uuid")

            # Append new records if any
            new_records_count = new_records_df.count()
            if new_records_count > 0:
                new_records_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table_name)
                print(f"🔄 Appended {new_records_count} new records to '{target_table_name}'.")
            else:
                print(f"✅ No new records to append for '{target_table_name}'.")
        else:
            # Create the Delta table if it does not exist
            source_df.write.format("delta").saveAsTable(target_table_name)
            print(f"✅ Created table '{target_table_name}' with all records from '{source_table_name}'.")

    except Exception as e:
        print(f"❌ Error processing table '{schema_name}.{table_name}': {str(e)}")

def process_schema(schema_name):
    """Processes all tables within a single schema."""
    
    # Determine the correct schema for writing
    target_schema = INFO_SCHEMA_TARGET if schema_name == "information_schema" else schema_name
    
    print(f"🔄 Processing schema: {schema_name} → Target Schema: {target_schema}")

    # Create the schema in the target catalog
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{target_schema}")
        print(f"✅ Schema '{TARGET_CATALOG}.{target_schema}' is ready.")
    except Exception as e:
        print(f"❌ Error creating schema '{target_schema}': {str(e)}")
        return

    # Retrieve all tables in the schema
    try:
        tables = spark.sql(f"SHOW TABLES IN {SOURCE_CATALOG}.{schema_name}").collect()
        table_names = [row['tableName'] for row in tables if row['tableName'] not in EXCLUDED_TABLES]
    except AnalysisException as e:
        print(f"❌ Error accessing schema '{schema_name}': {str(e)}")
        return

    # Process tables in parallel
    futures = []
    with ThreadPoolExecutor() as executor:
        for table_name in table_names:
            futures.append(executor.submit(process_table, schema_name, table_name))

        for future in as_completed(futures):
            future.result()

def main():
    """Main function to orchestrate the ingestion process across all schemas and tables."""
    
    create_catalog_if_not_exists(TARGET_CATALOG)

    # Retrieve all schemas
    schemas = spark.sql(f"SHOW SCHEMAS IN {SOURCE_CATALOG}").collect()
    schema_field = 'namespace' if 'namespace' in schemas[0].asDict() else 'databaseName'
    schema_names = [row[schema_field] for row in schemas if row[schema_field] not in EXCLUDED_SCHEMAS]

    # Ensure information_schema tables go into cog_information_schema
    if "information_schema" in schema_names:
        schema_names.append("information_schema")

    # Process schemas in parallel
    futures = []
    with ThreadPoolExecutor() as executor:
        for schema_name in schema_names:
            futures.append(executor.submit(process_schema, schema_name))

        for future in as_completed(futures):
            future.result()

    print("\n✅ Incremental ingestion process completed.")

if __name__ == "__main__":
    main()
