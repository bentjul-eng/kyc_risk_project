# kyc_risk_project
kyc-risk-project/
│
├── README.md                    # Visão geral do projeto e instruções para execução
├── .gitignore
│
├── data/                        # Dados simulados e resultados finais
│   ├── bronze/                  # Dados brutos (Delta - CSV Simuilado)
│   ├── silver/                  # Dados transformados com regras aplicadas
│   ├── gold/                    # Tabelas finais para análise (Delta/Parquet)
│   └── powerbi/                 # Arquivos para exportação ou visualização no Power BI
│
├── scripts/                     # Scripts principais do pipeline
│   ├── 01_generate_csvs.py      # Gera os CSVs simulados com Faker
│   ├── 02_ingest_data.py        # Lê os CSVs e salva como Delta (camada Bronze)
│   ├── 03_transform_data.py     # Aplica regras de risco e joins (camada Silver)
│   ├── 04_generate_summary.py   # Gera tabela final agregada (camada Gold)
│   └── 05_export_powerbi.py     # (Opcional) Converte Delta para Parquet se necessário
│
├── notebooks/                   # Notebooks Databricks para apresentação e testes
│   └── main_pipeline_demo.ipynb
│
├── docs/                        # Documentação do projeto
│   ├── discovery.md             # Tabelas, metadados, modelagem
│   ├── architecture.md          # Diagrama da arquitetura e camadas Delta
│   └── erd.png                  # Diagrama entidade-relacionamento
│
└── dashboard/                   # Dashboard Power BI
    └── kyc_dashboard.pbix       # (opcional) visualização de risco dos clientes
