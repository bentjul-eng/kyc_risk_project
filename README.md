# KYC Risk Project

---

This project aims to build a modular and scalable data pipeline for KYC (Know Your Customer) risk assessment, using data engineering tools such as Python, PySpark, and full integration with Azure Cloud services and Power BI for analytics.

---

## Project Structure

kyc_risk_project-main/
│
├── README.md # Main project description
│
├── docs/ # Project documentation
│ ├── 01_documetation_kyc.md
│ ├── fluxo_azure_ready.png
│ └── er_diagram.jpg
│
├── notebooks/ # Databricks notebooks for each pipeline stage
│ ├── 01_ingestion_raw_data.ipynb
│ ├── 02_processing_silver.ipynb
│ ├── 03_processing_gold.ipynb
│
├── scripts/ # Python modules for CLI-based pipeline execution
│ ├── 00_ingestion.py
│ ├── 01_utils.py
│ ├── 02_processing.py
│ └── 03_orchestration.py

---

##  Current Features

- Ingestion of simulated KYC data from structured CSVs  
- Risk classification pipeline using business rules  
- Data quality checks and transformation layers (Bronze, Silver, Gold)  
- Modular architecture that enables orchestration through notebooks or CLI  
- Visual ER diagram and Azure architecture draft

---

##  Usage

The pipeline was designed to be executed either:

- **Via CLI**, using the modular Python scripts
- **Orchestrated through Azure Data Factory (ADF)**, which runs the Databricks notebooks as pipeline activities

```bash
pip install -r requirements.txt
```
Note: The requirements.txt file should be generated once dependencies are finalized.

#### Future Improvements
To enhance automation, scalability, and AI-driven decision-making, future iterations of this project will include:

- Deployment via GitHub Actions using CI/CD workflows to Azure

- Full orchestration in Azure with integration between Data Factory, Databricks, and Azure Storage

- ML-based risk scoring using Azure Machine Learning

- Monitoring and logging with Azure Monitor and Log Analytics

- Secrets and environment config management via Azure Key Vault


#### Documentation
All relevant diagrams are available in the docs/ folder:

- KYC Documentation

- ER Diagram

- Azure Flow Design

> Author
Julia Bento


inkedIn | GitHub
