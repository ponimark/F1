

# 🏎️ F1 Data Analytics & Engineering Pipeline

## 📌 Overview

This project is an end-to-end **data engineering and analytics pipeline** built to ingest, process, and analyze **Formula 1 race data**.
It focuses on designing scalable data workflows, clean data models, and analytics-ready datasets that can support downstream analysis and visualization.

The pipeline simulates a real-world data platform used for sports analytics, combining **batch ingestion, transformation, and orchestration**.

---

## 🧱 Architecture

**High-level flow:**

```
External APIs (Ergast / Weather)
        ↓
Ingestion Layer (Python)
        ↓
Raw Storage
        ↓
Transformation Layer (DBT)
        ↓
Analytics Tables
        ↓
Insights / Dashboards / ML-ready datasets
```

---

## 🔧 Tech Stack

* **Python** – data ingestion, preprocessing
* **Apache Airflow** – workflow orchestration (DAGs)
* **DBT** – data modeling and transformations
* **PostgreSQL** – analytical storage
* **Docker & Docker Compose** – local environment setup
* **GitHub** – version control and collaboration

---

## 📂 Project Structure

```
F1/
├── dags/                  # Airflow DAG definitions
├── dbt/                   # DBT models and transformations
├── include/               # Shared utilities and helpers
├── config.py              # Configuration settings
├── docker-compose.yml     # Local orchestration
├── requirements.txt       # Python dependencies
├── devlog.md              # Development log and progress notes
└── README.md              # Project documentation
```

---

## 📊 Data Sources

* **Ergast API**

  * Race schedules
  * Driver and constructor standings
  * Lap times and results
* **External Weather APIs** (optional enrichment)

  * Track-level weather conditions
  * Race-day context

---

## 🔄 Pipeline Features

* Idempotent ingestion to avoid duplicate data
* Schema-driven transformations using DBT
* Analytics-friendly fact and dimension tables
* Modular and extensible pipeline design
* Dockerized local development environment

---

## 🧠 Example Analytics Use Cases

* Driver and constructor performance trends
* Lap time and pace analysis
* Pit stop strategy comparison
* Historical race comparisons
* Weather impact on race outcomes

---

## ▶️ How to Run Locally

1. Clone the repository:

   ```bash
   git clone https://github.com/ponimark/F1.git
   cd F1
   ```

2. Start services:

   ```bash
   docker-compose up
   ```

3. Access Airflow:

   ```
   http://localhost:8080
   ```

4. Run DBT models:

   ```bash
   dbt run
   ```

---

## 🚀 Future Improvements

* Streaming ingestion for live race data
* ML models for race pace prediction
* Visualization layer (Superset / Metabase)
* Enhanced data quality checks
* Cloud deployment (AWS/GCP)

---

## 📬 Notes

This project is intended to demonstrate **data engineering system design**, not just analytics queries.
The emphasis is on **scalability, maintainability, and clean data modeling**.

---

### ✅ Next steps (do this now)

1. Open `README.md`
2. Paste everything above
3. Save
4. Run:

```bash
git add README.md
git commit -m "Restore project README with architecture overview"
git push
```

