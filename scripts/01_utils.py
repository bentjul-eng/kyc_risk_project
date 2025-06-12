# 01_utils.py
from pyspark.sql import SparkSession

def load_bronze_data(spark, bronze_path):
    """Load Bronze tier data from CSV files"""
    clients_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{bronze_path}/clients.csv")
    )

    transactions_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{bronze_path}/transactions.csv")
    )

    high_risk_countries_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{bronze_path}/high_risk_countries.csv")
    )

    return clients_df, transactions_df, high_risk_countries_df


def save_to_silver(df, silver_path):
    """Salva os dados enriquecidos na camada Silver em formato Delta particionado por data."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("evaluation_date")
        .save(f"{silver_path}/client_transactions_risk")
    )


def save_to_gold(aggr_df, gold_path):
    """Salva os dados agregados na camada Gold em formato Delta."""
    (
        aggr_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/aggregated_client_risk")
                    )
