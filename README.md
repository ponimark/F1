# F1

# 🏎️ F1 Data Engineering Project  

## 📌 Overview  
This project is an **end-to-end data engineering pipeline for Formula 1 datasets**. It ingests, transforms, and models data from the [OpenF1 API](https://api.openf1.org), making it analytics-ready for performance analysis, race strategy insights, and reporting.  

The project was **initially implemented on Snowflake** but has since been **migrated to PostgreSQL** for local and development use. The modular design ensures compatibility with multiple warehouses.  

---

## ⚙️ Tech Stack  
- **Python** → API ingestion and ETL scripts  
- **Apache Airflow** → Orchestration of DAGs, scheduling and monitoring  
- **dbt (Data Build Tool)** → Transformations, modeling, testing, and documentation  
- **PostgreSQL** → Current analytics warehouse  
- **Snowflake** → Legacy warehouse, supported by dbt  
- **Docker Compose** → Reproducible environment for Airflow + Postgres  

---

## 🧩 dbt Layers  
The dbt project follows a standard layered approach:  

1. **Staging Layer (`stg_`)**  
   - Raw tables are cleaned and standardized.  
   - Handles column renaming, datatype casting, and null checks.  
   - Example: `stg_drivers`, `stg_laps`, `stg_sessions`.  

2. **Intermediate Layer (`int_`)**  
   - Combines staging models for richer transformations.  
   - Business logic like lap-to-session joins or pit stop analysis.  
   - Example: `int_intervals`, `int_pit_stops`.  

3. **Mart Layer (`final_`)**  
   - Final analytics-ready fact and dimension tables.  
   - Optimized for BI dashboards and downstream use cases.  
   - Example: `final_summary_leaderboard`, `final_stints`.  

4. **Seeds & Sources**  
   - Seed files provide static lookup tables (e.g., track metadata).  
   - Sources define external raw data connections.  

---

## 🔄 Data Flow Architecture  

```text
        ┌──────────────────┐
        │   OpenF1 API     │
        └───────┬──────────┘
                │
                ▼
        ┌──────────────────┐
        │   Python ETL     │
        │ (Ingestion Code) │
        └───────┬──────────┘
                │
                ▼
        ┌──────────────────┐
        │   Airflow DAGs   │
        │ Orchestration    │
        └───────┬──────────┘
                │
                ▼
        ┌──────────────────┐
        │ Raw Database     │
        │ (Postgres/Snowflake) │
        └───────┬──────────┘
                │
                ▼
        ┌──────────────────┐
        │ dbt Staging      │
        └───────┬──────────┘
                │
                ▼
        ┌──────────────────┐
        │ dbt Intermediate │
        └───────┬──────────┘
                │
                ▼
        ┌──────────────────┐
        │ dbt Marts        │
        │ (Facts & Dims)   │
        └───────┬──────────┘
                │
                ▼
        ┌──────────────────┐
        │ BI / Analytics   │
        │ (Power BI, SQL)  │
        └──────────────────┘
