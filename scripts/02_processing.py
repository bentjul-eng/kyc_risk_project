# processing.py
# Pure functions for data enrichment and aggregation

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

def enrich_risk(clients_df, transactions_df, high_risk_countries_df, evaluation_timestamp):
    """
    Enrichment of transaction data with risk assessment.
    """
    joined_df = (
        transactions_df.alias("t")
        .join(clients_df.alias("c"), on="client_id", how="inner")
        .join(
            high_risk_countries_df.alias("h").withColumnRenamed("country", "high_risk_country"),
            col("c.country") == col("h.high_risk_country"),
            how="left"
        )
        .withColumn("is_high_risk_country", col("h.high_risk_country").isNotNull())
    )

    risk_df = (
        joined_df
        .withColumn("is_high_value_transaction", when(col("transaction_amount") > 10000, lit(1)).otherwise(lit(0)))
        .withColumn("is_minor", when(col("age") < 18, lit(1)).otherwise(lit(0)))
        .withColumn(
            "risk_score",
            col("is_high_risk_country").cast("int") * 1 +
            col("is_high_value_transaction").cast("int") * 1.5 +
            col("is_minor").cast("int") * 1
        )
        .withColumn("risk_flag", when(col("risk_score") >= 2, lit(True)).otherwise(lit(False)))
        .withColumn("evaluation_timestamp", lit(evaluation_timestamp).cast("timestamp"))
        .withColumn("evaluation_date", to_date(col("evaluation_timestamp")))
        .withColumn("event_id", concat_ws("_", col("client_id"), col("transaction_id")))
    )

    return risk_df


def aggregate_by_client(risk_df):
    """
    Aggregation of risk data by client.
    """
    aggr_df = (
        risk_df.groupBy("client_id")
        .agg(
            count("*").alias("total_transactions"),
            _sum("transaction_amount").alias("total_amount"),
            _sum(when(col("risk_flag"), 1).otherwise(0)).alias("high_risk_transactions"),
            _max("risk_score").alias("max_risk_score"),
            _max("is_high_risk_country").alias("ever_high_risk_country"),
            _max("is_minor").alias("is_minor")
        )
        .withColumn("high_risk_ratio", col("high_risk_transactions") / col("total_transactions"))
    )

    return aggr_df

