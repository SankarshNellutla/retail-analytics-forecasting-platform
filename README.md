Retail Sales Analytics & Forecasting Platform
================================================

Author: Sankarsh Nellutla  
Date: 2025-11-09

Table of Contents
-----------------

1. Project Overview
2. Architecture and Data Flow
3. Data Lineage
4. Conceptual Mapping to Business Tables
5. Final Star Schema Models
6. ETL Summary
7. Analytics and Visualization
8. Machine Learning Forecasting
9. GenAI-Powered Chatbot
10. Next Steps
11. Repository Structure
12. Getting Started

Project Overview
----------------

This repository contains a complete data engineering and analytics solution for retail sales.  It uses the Kaggle Superstore dataset, which records orders, customers, products, and financial transactions for a fictional retail company.  The dataset includes nearly ten thousand rows and nineteen fields covering order dates, shipment dates, customer and product identifiers, sales, quantity, discounts and profits【941765044199307†screenshot】.  The objective is to clean and model this data, expose it through a star schema warehouse, analyse it through dashboards and predictive models, and allow ad hoc questions via a simple chatbot.

Goals
-----

- Ingest the raw Superstore CSV and standardize it using PySpark, loading it into staging tables in PostgreSQL.
- Design a star schema with dimension and fact tables (date, location, product, customer, order, sales) and populate it idempotently.
- Provide data profiling and DDL scripts so that staging and final schemas can be re-created from scratch.
- Build an interactive analytics dashboard (Power BI or similar) for sales, customer segments, product categories and regional performance.
- Train a ridge regression model to forecast next-month product sales at the product or month grain.
- Expose a safe language model backed chatbot that maps business questions to predefined SQL templates.

Architecture and Data Flow
--------------------------

| Layer            | Platform or Tool         | Description |
|------------------|---------------------------|-------------|
| Raw ingestion    | PySpark, Postgres        | `extract.py` reads the CSV, cleans numeric and date fields, computes deterministic hashes and writes to `stg.stg_superstore_raw`.  The script uses Spark for vectorized parsing and ensures idempotent loads. |
| Typed staging    | PySpark, Postgres        | `transform_stage.py` reads the raw table, normalizes dates, discounts and numeric values and produces typed staging tables: `stg_dim_location`, `stg_dim_product`, `stg_dim_customer`, `stg_orders` and `stg_sales_lines`.  All tables are overwritten on each run to keep the staging area predictable. |
| Final warehouse  | pandas, SQLAlchemy, Postgres | `transform_load_final.py` reads typed staging tables, computes unit prices, normalizes discounts, parses dates to dimension attributes, upserts dimension tables and loads the `fact_sales` table idempotently. |
| Analytics        | BI tool (Power BI)       | The final star schema feeds dashboards that slice sales by product category, customer segment, region, time period and other dimensions.  Example pages include sales trends, profitability, top customers and regional performance. |
| Machine learning | Scikit-learn             | The `ml.ipynb` notebook aggregates sales data by product and month and trains a ridge regression model to predict next-month sales. |
| Chatbot          | OpenAI API               | The `chatbot.ipynb` notebook demonstrates a simple retrieval augmented chatbot that uses predefined SQL templates to answer questions such as “total sales last quarter” or “top three products in the West region”. |

Data Lineage
------------

The ETL process starts with the raw CSV and ends with a star schema warehouse.  Key steps include raw ingestion into a single staging table, splitting into typed staging tables, and loading dimension and fact tables.  The fact table references dimensions for date, location, product, customer and orders.

Conceptual Mapping to Business Tables
-------------------------------------

| Business concept | Implemented tables                          | Description |
|------------------|---------------------------------------------|-------------|
| Orders           | `dim_order`, `stg_orders`                   | Contains order identifiers, order and ship dates, shipping mode and shipping location.  Date attributes are normalized via `dim_date`. |
| Customers        | `dim_customer`, `stg_dim_customer`          | Customer key, name and segment, with a foreign key to `dim_location`. |
| Products         | `dim_product`, `stg_dim_product`            | Product key, name, category and sub-category. |
| Locations        | `dim_location`, `stg_dim_location`          | Unique city, state, postal code and region combinations used for shipping and customer addresses. |
| Sales lines      | `fact_sales`, `stg_sales_lines`             | Grain of one order-line; references orders, customers, products and locations; stores quantity, sales, discount, unit price and profit. |
| Calendar         | `dim_date`                                  | Date dimension capturing year, quarter, month, day, day-of-week and weekend flag. |

Final Star Schema Models
------------------------

| Type        | Table         | Description |
|-------------|--------------|-------------|
| Dimension   | `dim_date`      | Calendar attributes and primary key `date_id`. |
| Dimension   | `dim_location`  | Surrogate `location_id` and natural keys for city, state, postal code and region. |
| Dimension   | `dim_product`   | Surrogate `product_id` plus product key, name, category and sub-category. |
| Dimension   | `dim_customer`  | Surrogate `customer_id` plus customer key, name, segment and location. |
| Dimension   | `dim_order`     | Surrogate `order_id` plus order key, date keys, ship mode, location and convenience year or month columns. |
| Fact        | `fact_sales`    | Grain: one order-line.  Stores quantity, sales, unit price, discount and profit, and references the five dimensions.  All monetary fields are numeric with defined precision. |

ETL Summary
-----------

| Step                     | Task                                                                      | Key points |
|-------------------------|---------------------------------------------------------------------------|-----------|
| Extract                 | Read and cleanse the raw CSV using PySpark (`extract.py`).  Strip currency symbols and commas, normalize discounts, compute SHA-256 row hashes and load rows into `stg.stg_superstore_raw`. | Uses Spark for performance; idempotent truncation preserves table structure. |
| Transform (typed staging) | Convert raw strings to correct types and shapes (`transform_stage.py`).  Derive typed staging tables for locations, products, customers, orders and sales lines.  Normalize dates and discount values. | All data remains in Spark until loaded to staging tables. |
| Load (final warehouse)  | Read typed staging tables with pandas (`transform_load_final.py`).  Compute unit price, normalize numeric columns, upsert dimension tables (`dim_*`) and load fact table with deletion of prior facts for affected orders. | Idempotent upserts via `ON CONFLICT` statements ensure repeatable loads. |
| Profiling               | `profile_superstore.py` computes basic null counts, unique counts and maximum string lengths for planning DDL sizes. | Aids schema design. |
| DDL                    | SQL scripts in `sql/ddl_staging.sql` and `sql/ddl_superstore_final.sql` create staging and final schemas with data types, constraints and indexes. | Aligns with ETL. |

Analytics and Visualization
---------------------------

The final star schema enables rich analytics.  Example dashboards include:

- **Sales summary** – aggregate sales, profit and quantity by month or quarter.  Visualize trends and seasonality.
- **Customer insights** – break down sales and profit by customer segments (Consumer, Corporate, Home Office) and identify top customers.
- **Product performance** – compare categories and sub-categories and identify high-margin products.
- **Regional analysis** – evaluate sales and profitability by region, state and city.
- **Order pipeline** – monitor order counts, average discount and shipping delays over time.

These dashboards can be built in Power BI or Tableau using the fact and dimension tables.  Because the warehouse uses a star schema, analysts can slice and dice metrics across any combination of dimensions.

Machine Learning Forecasting
---------------------------

To forecast future sales at the product or month grain, the project includes a regression model built with scikit-learn.  The pipeline aggregates fact table data into monthly product sales, encodes product and category features and trains a ridge regression model.  Ridge regression adds an L2 penalty term to the ordinary least squares objective, shrinking coefficients and helping reduce over-fitting, especially when predictors are correlated【941765044199307†screenshot】.

Workflow
--------

| Step            | Task                                                                               | Tools        |
|----------------|-------------------------------------------------------------------------------------|-------------|
| Data preparation | Aggregate `fact_sales` by product and month.  Create features such as lagged sales and rolling means; encode product category. | pandas, scikit-learn |
| Train/test split | Hold out the most recent months for testing.  Use the remaining data for training. | pandas |
| Model training   | Fit a `Ridge` regression model with cross-validated hyper-parameter alpha.          | scikit-learn |
| Evaluation       | Compute metrics: mean absolute error (MAE), root mean squared error (RMSE) and R². | scikit-learn.metrics |
| Forecast         | Use the trained model to predict next-month sales for each product.                 | scikit-learn |

Example metrics
---------------

| Metric     | Example value |
|-----------|---------------|
| MAE        | 12.8          |
| RMSE       | 19.4          |
| R²         | 0.72          |

GenAI-Powered Chatbot
----------------------

The project includes a minimal retrieval augmented chatbot that allows business users to ask natural language questions and receive answers backed by SQL queries.  The chatbot uses:

- A small embedding model to convert the question into a vector and search a set of predefined query templates.
- Template-driven SQL queries that prevent arbitrary code execution.  Questions such as “total sales for Q4 2019” or “top five products in the East region” are mapped to safe SQL with parameters.
- A lightweight text-generation model to summarise query results in a friendly way.

This design provides a safe and extensible starting point for conversational analytics without exposing the database to arbitrary instructions.

Next Steps
----------

- Enhance the forecasting model: experiment with time-series specific models (ARIMA, Prophet) and include seasonality and promotional features.
- Deeper customer analytics: build cohort analysis and customer lifetime value models.
- Add support for streaming data: integrate with real-time data sources and update the warehouse incrementally.
- Deploy the chatbot: convert the notebook into a web application using Streamlit and secure API keys via environment variables.
- Expand BI dashboards: design interactive reports for executives and operations teams.

Repository Structure
--------------------

```
src/
  etl/
    extract.py             # read and clean raw CSV into stg.stg_superstore_raw
    transform_stage.py     # convert raw staging into typed staging tables
    transform_load_final.py# upsert dimensions and load fact_sales
  profiling/
    profile_superstore.py  # basic profiling of the CSV
sql/
  ddl_staging.sql            # create staging schema and tables
  ddl_superstore_final.sql   # create final star-schema tables and helper views
notebooks/
  ml.ipynb                   # ridge regression model training and forecasting
  chatbot.ipynb              # simple GenAI chatbot demo
models/                      # saved model artefacts
images/                      # diagrams and dashboard screenshots
data/                        # placeholder – store CSV locally (never commit)
README.md (this file)
```

Getting Started
---------------

1. Clone the repository:

   ```bash
   git clone https://github.com/SankarshNellutla/retail-analytics-forecasting-platform.git
   cd retail-analytics-forecasting-platform
   ```

2. Set up the environment:

   - Install Python 3.9 or newer and create a virtual environment.
   - Copy `.env.example` to `.env` and provide values for `CSV_PATH`, `JDBC_URL`, `DB_USER`, `DB_PASS`, `PG_JDBC_JAR` and `DB_URL`.
   - Install dependencies:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. Run the ETL pipeline:

   Ensure that PostgreSQL is running and the `superstore` database exists.

   ```bash
   # create schemas and tables
   psql -f sql/ddl_staging.sql -d superstore
   psql -f sql/ddl_superstore_final.sql -d superstore

   # extract raw CSV into staging
   python src/etl/extract.py

   # transform raw staging into typed staging
   python src/etl/transform_stage.py

   # load typed staging into final warehouse
   python src/etl/transform_load_final.py
   ```

4. Train the forecasting model:

   Open `notebooks/ml.ipynb` in Jupyter or Databricks, run the cells to aggregate sales data, train the ridge regression model and evaluate its performance.  The notebook also shows how to use the model to predict next-month sales.

5. Run the chatbot demo:

   Open `notebooks/chatbot.ipynb`, set your OpenAI API key and follow the instructions to ask business questions.  The notebook demonstrates mapping natural language queries to safe SQL templates.

For contribution guidelines and license information, see the **CONTRIBUTING.md** and **LICENSE** files in this repository.
