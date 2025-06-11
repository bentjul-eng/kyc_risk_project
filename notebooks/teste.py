import pandas as pd
import random
from datetime import datetime, timedelta

# --- Parameters ---
n_clients = 15
n_transactions = 50
n_high_risk_countries = 5

# --- Database---
names = [
    'Carlos L.', 'Sofia R.', 'Lucas M.', 'Isabela F.', 'Gabriel A.',
    'Laura C.', 'Mateus G.', 'Júlia S.', 'Pedro R.', 'Beatriz S.',
    'John S.', 'Maria G.', 'James J.', 'Patricia B.', 'Robert J.'
]

countries = [
    'Brazil', 'Portugal', 'Spain', 'China', 'Oman',
    'Germany', 'India', 'Russia', 'Argentina', 'Mexico',
    'Iran', 'Venezuela', 'Myanmar', 'Lebanon', 'North Korea'
]

age_range = (18, 75)

# ---  Generate clients ---
clients = []
for i in range(1, n_clients + 1):
    clients.append({
        'client_id': i,
        'name': random.choice(names),
        'age': random.randint(*age_range),
        'country': random.choice(countries)
    })

clients_df = pd.DataFrame(clients)
#print(clients_df.head())

# --- generate transactions ---
transactions = []
for i in range(1, n_transactions + 1):
    client = random.choice(clients)
    amount = round(random.uniform(10.0, 5000.0), 2)
    days_ago = random.randint(0, 365)
    date = (datetime.now() - timedelta(days=days_ago)).date()

    transactions.append({
        'transaction_id': i,
        'client_id': client['client_id'],
        'transaction_amount': amount,
        'transaction_date': str(date)
    })

transactions_df = pd.DataFrame(transactions)
#print(transactions_df.head())

# --- 3. Gerar high_risk_countries ---
high_risk_sample = random.sample(countries, n_high_risk_countries)
high_risk_df = pd.DataFrame({'country_name': high_risk_sample})
#print(high_risk_df.head())

# --- 4. Converter para Spark DataFrames ---
df_clients_spark = spark.createDataFrame(clients_df)
df_transactions_spark = spark.createDataFrame(transactions_df)
df_high_risk_spark = spark.createDataFrame(high_risk_df)

# --- 5. Salvar como CSV no DBFS ---
df_clients_spark.write.mode("overwrite").option("header", True).csv("/tmp/clients")
df_transactions_spark.write.mode("overwrite").option("header", True).csv("/tmp/transactions")
df_high_risk_spark.write.mode("overwrite").option("header", True).csv("/tmp/high_risk_countries")
