# KYC Risk Project
---

This project aims to create a data pipeline for KYC (Know Your Customer) risk assessment, using data engineering tools such as Python, Pyspark and integration with Azure Cloud and PowerBI.

## Project Structure

kyc_risk_project-main/
│
├── README.md # Main project description
│
├── docs/ # Project documentation
│   ├── 01_documetation_kyc.md
│   ├── fluxo_azure_ready.png
│   └── er_diagram.jpg
│
├── notebooks/ # Test notebooks for each pipeline stage
│   ├── 01_ingestion_test.ipynb
│   ├── 02_processing_test.ipynb
│   ├── 03_orchestration_test.ipynb
│
├── scripts/ # Python scripts for ingestion, processing, and orchestration
│   ├── 00_ingestion.py
│   ├── 01_utils.py
│   ├── 02_processing.py
│   └── 03_orchestration.py


---

## Features

- Ingestion of simulated KYC data  
- Risk processing and score generation  
- Modular pipeline orchestration  
- Architecture and entity-relationship (ER) model visualization  

---

```bash
pip install -r requirements.txt
```
Note: There is no requirements.txt file included. You can generate it after setting up the environment.

#### Documentation
Architecture diagrams and flow are available in the docs/ folder, including:
- KYC Documentation

- ER Diagram

- Azure Cloud Flow

### Author
Julia Bento