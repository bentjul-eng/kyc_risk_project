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

bronze_path = "dbfs:/Workspace/Users/jusoares_flor@hotmail.com/kyc_risk_project/data/csv_sources"
SILVER_PATH = "dbfs:/mnt/datalake/silver/kyc_risk_analysis"
GOLD_PATH = "dbfs:/mnt/datalake/gold/kyc_risk_analysis"
EVALUATION_DATE = "2025-06-10"


bronze_path = "dbfs:/Workspace/Users/jusoares_flor@hotmail.com/kyc_risk_project/data/csv_sources"

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

from pyspark.sql.functions import col

joined_df = (
    transactions_df.alias("t")                               # transações
    .join(clients_df.alias("c"), on="client_id", how="inner") # + clientes
    .join(                                                   # + países de risco
        high_risk_countries_df.alias("h")
            .withColumnRenamed("country", "high_risk_country"),
        col("c.country") == col("h.high_risk_country"),
        how="left"
    )
    .withColumn("is_high_risk_country", col("h.high_risk_country").isNotNull())
)

display(joined_df)


from pyspark.sql.functions import col, when, lit, to_date, concat_ws

# Calcular colunas de risco no DataFrame
risk_df = joined_df \
    .withColumn(
        "is_high_value_transaction",
        when(col("transaction_amount") > 10000, lit(1)).otherwise(lit(0))
    ) \
    .withColumn(
        "is_minor",
        when(col("age") < 18, lit(1)).otherwise(lit(0))
    ) \
    .withColumn(
        "risk_score",
        col("is_high_risk_country").cast("int") * 1 +
        col("is_high_value_transaction").cast("int") * 1.5 +
        col("is_minor").cast("int") * 1
    ) \
    .withColumn(
        "risk_flag",
        when(col("risk_score") >= 2, lit(True)).otherwise(lit(False))
    ) \
    .withColumn(
        "evaluation_timestamp",
        to_date(lit("2025-06-10"))
    ) \
    .withColumn(
        "event_id",
        concat_ws(col("client_id"), col("transaction_id"))
    )

display(risk_df)

(risk_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_PATH}/client_transactions_risk"))

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
   
display(aggr_df)

(
        aggr_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{GOLD_PATH}/aggregated_client_risk")
    )



display(high_risk_countries_df)