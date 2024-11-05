# Wind Turbine Data Processing Pipeline

This project implements a data processing pipeline for a renewable energy company's wind turbine farm. The pipeline uses a medallion architecture to process the data, from Bronze -> Silver -> gold & gold_summary. It's built to handle null values, detect anomalies in power output, and provide statistical analysis of power output. 

# Project Structure
```
turbine-task/
├── src/                         # Source code
│   ├── data_ingestion.py        # Data ingestion
│   ├── data_cleansing.py        # Data cleansing & preprocessing
│   ├── data_transformation.py   # Statistical analysis and anomaly detection
│   └── database.py              # Database creation & operations
├── tests/                       # Unit tests
├── data/                        # Raw data location (not included in this repo)
└── pipeline.py                  # Project Entrypoint, contains end-to-end data pipeline
```

# Design Decisions

- A Medallion architecture will be used.
- Null values will be dropped rather than computed.
- Tables will be overwritten rather than appended to.
- Pytest to be used as the testing suite.
- Logging and exception handling implemented on pipeline code.

