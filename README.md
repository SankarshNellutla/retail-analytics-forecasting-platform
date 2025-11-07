# Superstore — Retail Sales Analytics & Forecasting

**Short:** end-to-end data engineering pipeline + ML forecasting + simple gen-AI chatbot using the Superstore dataset.

This repo implements:
- ETL (PySpark) to clean CSV → typed staging in Postgres.
- Final load (pandas + SQLAlchemy) to upsert dimensions and idempotently load `fact_sales`.
- A Ridge regression model to forecast next-month product sales (per-product monthly).
- A minimal LLM-backed chatbot (safe, pre-defined SQL templates) for business queries.
- DDL scripts for staging and final schema (Postgres).

---
## Quick links
- `src/etl/` — `extract.py`, `transform_stage.py`, `transform_load_final.py`
- `src/profiling/` — `profile_superstore.py`
- `notebooks/` — `ml.ipynb`, `chatbot.ipynb`
- `sql/` — `ddl_staging.sql`, `ddl_superstore_final.sql`
- `models/` — saved model pipeline (small artifacts only)
- `images/` — diagrams (DDL, schema, dashboard screenshot)
- `data/` — **DO NOT** commit full dataset. Use `.env` to point to local copy.

---

## Getting started (minimal, safe)

1. **Clone repo**
```bash
git clone git@github.com:<your-username>/<repo>.git
cd <repo>