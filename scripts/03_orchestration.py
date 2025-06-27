"""
orchestration_uc.py – Orchestrates the full KYC Risk Analysis pipeline using Unity Catalog
"""

from pyspark.sql import SparkSession
from processing import enrich_risk, aggregate_by_client

# -----------------------
# Step 1: Widgets for Catalog and Schema
# -----------------------
dbutils.widgets.text("catalog", "governance_risk")
dbutils.widgets.text("schema", "kyc_project")
dbutils.widgets.text("evaluation_timestamp", "2025-06-10 12:00:00")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
evaluation_timestamp = dbutils.widgets.get("evaluation_timestamp")

# -----------------------
# Step 2: Start Spark and validate context
# -----------------------
spark = SparkSession.builder.appName("kyc-risk-orchestration").getOrCreate()

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
if catalog not in catalogs:
    raise Exception(f"Catalog '{catalog}' not found. Please create it in the Unity Catalog UI.")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE SCHEMA {schema}")

# -----------------------
# Step 3: Load Silver data from Unity Catalog
# -----------------------
silver_table = f"{catalog}.{schema}.client_transactions_risk"
tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()]
if "client_transactions_risk" not in tables:
    raise Exception(f"Silver table '{silver_table}' not found. Run the processing script first.")

risk_df = spark.read.table(silver_table)

# -----------------------
# Step 4: Aggregate to Gold
# -----------------------
aggr_df = aggregate_by_client(risk_df)

aggr_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{schema}.aggregated_client_risk")

print("✅ Gold table 'aggregated_client_risk' successfully written to Unity Catalog.")
