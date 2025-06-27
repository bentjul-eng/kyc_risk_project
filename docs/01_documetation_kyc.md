# Discovery – KYC Risk Analysis Pipeline

This document outlines the initial definitions for the simplified Know Your Customer (KYC) risk analysis project, with a focus on risk prevention. It covers the source tables (Bronze), enrichment logic (Silver), final aggregations (Gold), and decisions.

---

## Pipeline Objective

Develop a data pipeline for ingestion, transformation, and aggregation of customer and transaction data, focusing on risk evaluation and flagging based on business rules such as:

- High-value transactions  
- Underage customers  
- High-risk countries 
---

##  Source Data (CSV Files)

Synthetic CSV files used as the input layer for data ingestion.

### 1. `clients.csv` – Customer Information

| Column     | Type   | Description                        |
|------------|--------|------------------------------------|
| client_id  | INT    | Unique customer identifier         |
| first_name| STRING | First name                          |
| last_name | STRING | Last Name
| date_birth | DATE    | Customer's Date Birth             |
| residency_country    | STRING | Country of residence               |

---

### 2. `transactions.csv` – Transaction Details

| Column              | Type   | Description                               |
|---------------------|--------|-------------------------------------------|
| transaction_id      | INT    | Unique transaction identifier             |
| client_id           | INT    | Foreign key referencing the customer      |
| transaction_amount  | DECIMAL(15,2)  | Value of the transaction                  |
| transaction_date    | DATE   | Date the transaction occurred             |

---

### 3. `high_risk_countries.csv` – High-Risk Country List

| Column   | Type   | Description                   |
|----------|--------|-------------------------------|
| high_risk_country  | STRING | Name of a high-risk country   |

---

## Silver Layer – Enriched Data

Intermediate table where each transaction is enriched with risk-related variables, flags, and a calculated score.

### Table: `client_transactions_risk` (Delta Format)

| Column                    | Type     | Description                                                                |
|---------------------------|----------|----------------------------------------------------------------------------|
| client_id                 | INT      | Customer identifier                                                        |
| transaction_id            | INT      | Transaction identifier                                                     |
| date_birth                       | INT      | Customer date birth                                                             |
| country                   | STRING   | Country of residence                                                       |
| transaction_amount        | DECIMAL(15,2)    | Transaction value                                                          |
| is_high_risk_country      | BOOLEAN  | True if the country is listed as high-risk                   |                                     
| risk_score                | DECIMAL(3,2)    | Calculated score based on the above variables                              |
| risk_flag                 | BOOLEAN  | True if the score is 2 or higher                                           |
| evaluation_timestamp      | DATE     | Evaluation date                                                            |
| event_id                  | STRING   | Unique risk event ID (concatenation of client_id and transaction_id)       |

**Scoring Rules:**
- High-risk country: +1  
- Transaction > 10,000: +1.5  
- Underage customer: +1  
- *A transaction is flagged if the total score ≥ 2*

---

## Gold Layer – Aggregated Risk Summary

Final consolidated table with client-level risk indicators.

### Table: `aggregated_client_risk` (Delta Format)

| Column                 | Type     | Description                                                   |
|------------------------|----------|---------------------------------------------------------------|
| client_id              | INT      | Customer identifier                                           |
| total_transactions     | INT      | Total number of transactions                                  |
| total_amount           | DECIMAL(15,2)    | Sum of all transaction amounts                                |
| total_high_risk_transactions | INT      | Number of transactions flagged as high-risk                  |
| max_risk_score         | DECIMAL(3,2)    | Highest risk score observed                                   |
| ever_high_risk_country | BOOLEAN  | True if the customer ever transacted from a high-risk country |
| high_risk_ratio        | DECIMAL(5,2)    | Ratio of high-risk transactions (high_risk / total)           |

