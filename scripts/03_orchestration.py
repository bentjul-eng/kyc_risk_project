"""
orchestration.py – KYC Risk Analysis project orchestration pipeline
"""

from pyspark.sql import SparkSession
from processing import enrich_risk, aggregate_by_client
from utils import load_bronze_data, save_to_silver, save_to_gold

# -----------------------
# General settings
# -----------------------

bronze_path = "dbfs:/Workspace/Users/jusoares_flor@hotmail.com/kyc_risk_project/data/csv_sources"
silver_path = "dbfs:/mnt/datalake/silver/kyc_risk_analysis"
gold_path = "dbfs:/mnt/datalake/gold/kyc_risk_analysis"
evaluation_timestamp = "2025-06-10 12:00:00" 


# -----------------------
# Main orchestration
# -----------------------

def orchestrate():
    spark = SparkSession.builder.appName("kyc-risk-pipeline").getOrCreate()

    # Bronze: Load CSVs
    clients_df, transactions_df, high_risk_countries_df = load_bronze_data(spark, bronze_path)

    # Silver: Enrich with risk logic
    risk_df = enrich_risk(clients_df, transactions_df, high_risk_countries_df, evaluation_timestamp)
    save_to_silver(risk_df, silver_path)

    # Gold: Aggregate risk per client
    aggr_df = aggregate_by_client(risk_df)
    save_to_gold(aggr_df, gold_path)


if __name__ == "__main__":
    orchestrate()





