# Retail Analytics and Forecasting Platform

A robust, fully open-source, end-to-end platform for analytics, warehousing, machine learning forecasting, and conversational insights on the Superstore retail dataset.  
ETL built with PySpark, warehousing on PostgreSQL, analysis with SQL, ML forecasting in scikit-learn, and a lightweight SQL+OpenAI-powered chatbot.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Repository Structure](#repository-structure)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Database Setup](#database-setup)
- [Schema Creation](#schema-creation)
- [ETL Workflow](#etl-workflow)
- [SQL Analysis](#sql-analysis)
- [Machine Learning & Modeling](#machine-learning--modeling)
- [Tableau Dashboards](#tableau-dashboards)
- [Chatbot Usage](#chatbot-usage)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Credits](#credits)
- [License](#license)

---

## Project Overview

The **Retail Analytics and Forecasting Platform** is a local, modular, and extensible data solution designed for retail sales analytics.  
It utilizes the well-known [Superstore](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) dataset and provides:

- Automated ETL from CSV (PySpark) to PostgreSQL (staging and star schemas)
- Analytical SQL scripts for core retail KPIs and trends
- Predictive modeling of next-month product sales with scikit-learn Ridge regression
- A minimal chatbot for SQL query templating and summary via LLM (optional OpenAI integration)
- Example Tableau dashboards

---

## Features

- **PySpark ETL**: Extracts, profiles, and cleans raw CSV data, storing in typed staging tables in PostgreSQL using a JDBC driver.
- **Data Warehouse**: Builds a snowflake/star schema with dimension and fact tables, fully reproducible via versioned DDL scripts.
- **SQL Analytics**: Out-of-the-box SQL for common retail analyses (KPI, cohort, RFM, cohort, etc.).
- **Machine Learning**: End-to-end pipeline in a Jupyter notebook for monthly, per-product demand prediction.
- **Chatbot**: Interactive Q&A notebook that supports safe template-based SQL and LLM-powered summaries (OpenAI optional).
- **Dashboards**: Sample Tableau dashboards and schema visualizations (image assets included).

---

## Repository Structure

```
retail-analytics-forecasting-platform/
├── .env.example                # Template for environment variables
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
├── README.md
├── requirements.txt            # Python dependencies
├── data/
│   └── Sample - Superstore.csv
├── Images/
│   ├── DDL_superstore_db.png
│   ├── DDL_staging.png
│   └── retail_sales_analytics_dashboard.jpeg
├── notebooks/
│   ├── chatbot.ipynb
│   ├── ml.ipynb
│   └── models/
│       └── next_month_sales_ridge.joblib
├── sql/
│   ├── ddl_staging.sql
│   ├── ddl_superstore_final.sql
│   └── test.sql
└── src/
    ├── etl/
    │   ├── extract.py
    │   ├── transform_stage.py
    │   └── transform_load_final.py
    └── profiling/
        └── profile_superstore.py
```

## Data Warehouse Star Schema

![Star Schema](Images/DDL_superstore_db.png)
*Figure: Logical star schema for the retail data warehouse*

---

## Quick Start

1. **Clone this repository:**
    ```bash
    git clone https://github.com/SankarshNellutla/retail-analytics-forecasting-platform.git
    cd retail-analytics-forecasting-platform
    ```
2. **Prepare a `.env` file:**  
   Copy `.env.example` to `.env` and fill in your secure configuration ([see instructions](#environment-setup)).
3. **Install requirements:**  
   Ensure Python 3.10+ and Java 17 are installed, then:
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
4. **Set up PostgreSQL (locally on port 5433 by default):**  
   See [Database Setup](#database-setup).
5. **Run schema DDLs:**  
   See [Schema Creation](#schema-creation).
6. **Run the ETL pipeline in order:**  
   See [ETL Workflow](#etl-workflow).
7. **Explore SQL, ML, and chatbot Jupyter notebooks:**  
   See [notebooks/](notebooks/).

---

## Prerequisites

- Python 3.10 or newer
- Java 17 (required by PySpark)
- PostgreSQL (default: localhost:5433)
- Postgres JDBC driver (declare absolute path in your `.env`)
- Local copy of Superstore dataset (`data/Sample - Superstore.csv`)
- Optional: OpenAI account/API key for chatbot advanced summaries
- Optional: Tableau for dashboard viewing

**Tip:** Example Java installation for macOS:
```bash
brew install openjdk@17
/usr/libexec/java_home -V
java -version
```

---

## Environment Setup

**Python & Java**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Java 17 for Spark**  
Choose ONE:
- Export before each session:
    ```bash
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    export PATH=$JAVA_HOME/bin:$PATH
    ```
- Or add the above to your `.venv/bin/activate` script.

**Environment Variables**  
Copy `.env.example` as `.env` and edit as needed:
```env
DB_USER=your_db_user
DB_PASS=your_db_password
DB_HOST=localhost
DB_PORT=5433
DB_NAME=superstore_db
DB_URL=postgresql+psycopg2://your_db_user:your_db_password@localhost:5433/superstore_db
JDBC_URL=jdbc:postgresql://localhost:5433/superstore_db
CSV_PATH=data/Sample - Superstore.csv
PG_JDBC_JAR=sql/lib/postgresql-42.7.8.jar
OPENAI_API_KEY=  # Optional
```
**⚠️ Never commit `.env` or any secrets to version control. Always keep `.env` in `.gitignore`.**

---

## Database Setup

**Create role and DB** (edit names/passwords to suit your environment):
```sql
psql -h localhost -p 5433 -U postgres -c "CREATE ROLE superstore_user WITH LOGIN PASSWORD 'your_password';"
psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE superstore_db OWNER superstore_user;"
```
**Secure your DB:**
```sql
psql -h localhost -p 5433 -U postgres -d superstore_db -c "REVOKE CONNECT ON DATABASE superstore_db FROM PUBLIC;"
psql -h localhost -p 5433 -U postgres -d superstore_db -c "GRANT CONNECT ON DATABASE superstore_db TO superstore_user;"
psql -h localhost -p 5433 -U postgres -d superstore_db -c "ALTER SCHEMA public OWNER TO superstore_user;"
psql -h localhost -p 5433 -U postgres -d superstore_db -c "REVOKE CREATE ON SCHEMA public FROM PUBLIC;"
psql -h localhost -p 5433 -U postgres -d superstore_db -c "GRANT ALL ON SCHEMA public TO superstore_user;"
```
**Set port and roles according to your needs; 5433 is the default in this template.**

---

## Schema Creation

**Apply schema scripts (in order):**
```bash
psql -h localhost -p 5433 -U superstore_user -d superstore_db -f sql/ddl_staging.sql
psql -h localhost -p 5433 -U superstore_user -d superstore_db -f sql/ddl_superstore_final.sql
```
- `ddl_staging.sql`: Sets up staging tables for incoming data.
- `ddl_superstore_final.sql`: Defines star schema with facts/dimensions.

---

## ETL Workflow

**1. Profile Data**
```bash
python src/profiling/profile_superstore.py
```
- Inspects nulls/uniques/ranges to inform schema.

**2. Extract to Raw Staging**
```bash
python src/etl/extract.py
```
- Reads, processes, and loads the CSV to `stg.stg_superstore_raw`.

**3. Transform to Typed Staging**
```bash
python src/etl/transform_stage.py
```
- Creates separated tables: location/product/customer/orders/sales_lines.

**4. Build the Star Schema**
```bash
python src/etl/transform_load_final.py
```
- Upserts dimensions, loads facts via delete-then-insert.

---

## SQL Analysis

Find ready-to-run queries in `sql/test.sql`.  
Examples include:
- Sales/profit by region/customer/product/ship mode/month
- Orders and quantities summary
- Monthly sales/profit trend

Sample query:
```sql
SELECT l.region, COUNT(DISTINCT o.order_id) AS orders, SUM(fs.quantity) AS total_qty,
  ROUND(SUM(fs.sales)::numeric, 2) AS total_sales
FROM fact_sales fs
JOIN dim_location l ON fs.location_id = l.location_id
JOIN dim_order o ON fs.order_id = o.order_id
GROUP BY l.region
ORDER BY total_sales DESC;
```
You can run any of these with:
```bash
psql -h localhost -p 5433 -U superstore_user -d superstore_db -f sql/test.sql
```
And analyze with your favorite SQL tool.

---

## Machine Learning & Modeling

- **ML notebook:** `notebooks/ml.ipynb`
- **Goal:** Predict next-month sales per product using time windows, lags, product attributes, and calendar features.
- **Pipeline:** scikit-learn, Ridge regression, with preprocessing steps for retail time series.
- **Model artifact:** Stored as `notebooks/models/next_month_sales_ridge.joblib`

**Metrics:**  
- Test rows: 84  
- MAE: ~106  
- RMSE: ~177  
- R²: ~0.33

---

## Tableau Dashboards

- Find sample Tableau dashboards/diagram screenshots in `Images/`.
- Example metrics: YoY sales, profit by region, moving averages, top customer/product, and RFM segmentation.

---

## Chatbot Usage

- **Notebook:** `notebooks/chatbot.ipynb`
- Supports a small set of safe analytic intents:
    - "Show sales by region"
    - "Top customers"
    - "Top products by profit"
    - "Sales/profit by ship mode"
- **How to use:**
    ```python
    run_chat_query('Show sales by region')
    ```
- Optionally enables OpenAI summarization if `OPENAI_API_KEY` is set. No key needed for basic SQL Q&A.

---

## Troubleshooting

- Double-check Java 17 is active for Spark (`java -version`).
- `PG_JDBC_JAR` path must be correct in the `.env`.
- Ensure your database URL/port/user matches in `.env`.
- If Spark writes fail, make sure DB and tables exist.
- If shapes/numbers are off, rerun `profile_superstore.py`.
- Chatbot supports only built-in intents—see notebook for details.

---

## Contributing

Contributions are welcome!  
See the [CONTRIBUTING.md](./CONTRIBUTING.md) for workflow, issue creation, and coding guidelines.

---

## Credits

- **Dataset:** Superstore dataset by vivek468 on Kaggle.
- **Author:** Sankarsh Nellutla ([GitHub](https://github.com/SankarshNellutla))
- **ML, SQL, and architectural templates:** Inspired by real-world retail data warehousing practices.

---

## License

[MIT](./LICENSE)
