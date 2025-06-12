from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
import random
from datetime import datetime, timedelta
import os

# --- Parameters ---
n_clients = 150
n_transactions = 500
n_high_risk_countries = 10

# --- Paths ---
output_path = "dbfs/Workspace/Users/jusoares_flor@hotmail.com/kyc_risk_project/data/csv_sources"
dbutils.fs.mkdirs(output_path)

# --- Database ---
names = [
    'Carlos L.', 'Sofia R.', 'Lucas M.', 'Isabela F.', 'Gabriel A.',
    'Laura C.', 'Mateus G.', 'Júlia S.', 'Pedro R.', 'Beatriz S.',
    'John S.', 'Maria G.', 'James J.', 'Patricia B.', 'Robert J.',
    'Wei C.', 'Li N.', 'Jing W.', 'Yang L.', 'Wei Z.',
    'Fatima A.', 'Mohammed A.', 'Ahmed A.', 'Aisha A.'
]

countries = [
    'Brazil', 'Portugal', 'Spain', 'China', 'Oman',
    'Germany', 'India', 'Russia', 'Argentina', 'Mexico',
    'Iran', 'Venezuela', 'Myanmar', 'Lebanon', 'North Korea'
]

age_range = (15, 75)

# --- Generate clients ---
clients = []
for i in range(1, n_clients + 1):
    clients.append((
        i,
        random.choice(names),
        random.randint(*age_range),
        random.choice(countries)
    ))

#Defines schema for the clients dataframe
clients_schema = StructType([
    StructField("client_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("country", StringType(), False)
])

clients_df = spark.createDataFrame(clients, schema=clients_schema)

#Save CSV - to save exactly as plain CSV, disable header in folder and save partitioned, use overwrite mode
clients_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/clients.csv")

# --- Generate transactions ---
transactions = []
for i in range(1, n_transactions + 1):
    client = random.choice(clients)
    amount = round(random.uniform(10.0, 5000.0), 2)
    days_ago = random.randint(0, 365)
    date = (datetime.now() - timedelta(days=days_ago)).date()

    transactions.append((
        i,
        client[0],  # client_id
        amount,
        date.isoformat()
    ))

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("client_id", IntegerType(), False),
    StructField("transaction_amount", FloatType(), False),
    StructField("transaction_date", StringType(), False)
])

transactions_df = spark.createDataFrame(transactions, schema=transactions_schema)
transactions_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/transactions.csv")

# --- Generate high_risk_countries ---
high_risk_sample = random.sample(countries, n_high_risk_countries)
high_risk = [(c,) for c in high_risk_sample]

high_risk_schema = StructType([
    StructField("country_name", StringType(), False)
])

high_risk_df = spark.createDataFrame(high_risk, schema=high_risk_schema)
high_risk_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/high_risk_countries.csv")























