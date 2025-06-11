"""
orchestration.py – KYC Risk Analysis project orchestration pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    to_date,
    concat_ws,
    sum as _sum,
    count,
    max as _max
)

# -----------------------
# General settings
# -----------------------

BRONZE_PATH = "dbfs:/Workspace/Users/jusoares_flor@hotmail.com/kyc_risk_project/data/csv_sources"
SILVER_PATH = "dbfs:/mnt/datalake/silver/kyc_risk_analysis"
GOLD_PATH = "dbfs:/mnt/datalake/gold/kyc_risk_analysis"
EVALUATION_DATE = "2025-06-10"

# -----------------------
# auxiliary functions
# -----------------------

def load_raw():
    """Loads Bronze tier CSVs and returns PySpark DataFrames."""
    clients_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BRONZE_PATH}/clients.csv")
    )

    transactions_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BRONZE_PATH}/transactions.csv")
    )

    high_risk_countries_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BRONZE_PATH}/high_risk_countries.csv")
    )

    return clients_df, transactions_df, high_risk_countries_df


def enrich_risk(clients_df, transactions_df, high_risk_countries_df):
    """Enriches transactions with risk scores and KYC flags."""

    joined_df = (
        transactions_df.alias("t")
        .join(clients_df.alias("c"), on="client_id", how="inner")
        .join(
            high_risk_countries_df.alias("h").withColumnRenamed("country", "high_risk_country"),
            col("c.country") == col("h.high_risk_country"),
            how="left",
        )
        .withColumn("is_high_risk_country", col("h.high_risk_country").isNotNull())
    )

    risk_df = (
        joined_df
        .withColumn(
            "is_high_value_transaction",
            when(col("transaction_amount") > 10000, lit(1)).otherwise(lit(0)),
        )
        .withColumn("is_minor", when(col("age") < 18, lit(1)).otherwise(lit(0)))
        .withColumn(
            "risk_score",
            col("is_high_risk_country").cast("int") * 1
            + col("is_high_value_transaction").cast("int") * 1.5
            + col("is_minor").cast("int") * 1,
        )
        .withColumn("risk_flag", when(col("risk_score") >= 2, lit(True)).otherwise(lit(False)))
        .withColumn("evaluation_timestamp", to_date(lit(EVALUATION_DATE)))
        .withColumn("event_id", concat_ws("_", col("client_id"), col("transaction_id")))
    )

    return risk_df


def save_to_silver(risk_df):
    """enriched DataFrame in Silver layer as Delta"""
    (
        risk_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_PATH}/client_transactions_risk")
    )


def aggregate_by_client(risk_df):

    aggr_df = (
        risk_df.groupBy("client_id")
        .agg(
            count("*").alias("total_transactions"),
            _sum("transaction_amount").alias("total_amount"),
            _sum(when(col("risk_flag"), 1).otherwise(0)).alias("high_risk_transactions"),
            _max("risk_score").alias("max_risk_score"),
            _max("is_high_risk_country").alias("ever_high_risk_country"),
            _max("is_minor").alias("is_minor"),
        )
        .withColumn("high_risk_ratio", col("high_risk_transactions") / col("total_transactions"))
    )

    return aggr_df


def save_to_gold(aggr_df):
    (
        aggr_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{GOLD_PATH}/aggregated_client_risk")
    )


# -----------------------
# main orchestration
# -----------------------

def orchestrate():
    clients_df, transactions_df, high_risk_countries_df = load_raw()
    risk_df = enrich_risk(clients_df, transactions_df, high_risk_countries_df)

    # Silver
    save_to_silver(risk_df)

    # Gold – aggregation
    aggr_df = aggregate_by_client(risk_df)
    save_to_gold(aggr_df)


if __name__ == "__main__":
    orchestrate()



