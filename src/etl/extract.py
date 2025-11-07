"""
extract.py

GOAL
 - Read raw CSV (local file), clean & normalize it in Spark, compute a deterministic row_hash,
   and load it into Postgres staging table stg.stg_superstore_raw.
 - If the staging table already exists, TRUNCATE it and replace rows (preserve table schema).

KEY DESIGN DECISIONS (WHY)
 - PySpark-first: leverage Spark SQL and expressions for robust parsing, vectorized transforms, and to
   demonstrate PySpark skill. Converting to pandas is avoided for DB writes.
 - Deterministic row_hash (sha256 over a fixed column order) for dedupe/audit downstream.
 - All numeric sanitization (strip $ and commas; normalize discount; rounding) happens in Spark to keep
   transformations centralized and fast.
 - Write strategy: overwrite by TRUNCATE (mode="overwrite", option("truncate","true")) ensures idempotence
   while preserving table DDL (good for staging tables managed separately by DDL scripts).
 - Encoding: convert Windows-1252 -> UTF-8 via a temp copy to avoid mojibake.
 - Resource safety: always remove temp file and stop Spark in finally block.

REQUIREMENTS (set in .env)
 - JDBC_URL  = jdbc:postgresql://localhost:5433/superstore_db
 - DB_USER   = superstore_user
 - DB_PASS   = superman
 - PG_JDBC_JAR = /path/to/postgresql-42.7.8.jar
 - CSV_PATH  = data/Sample - Superstore.csv

USAGE
  source .venv/bin/activate
  python src/etl/extract.py
"""

import os
import tempfile
import shutil
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    trim, col, coalesce, lit, concat_ws, sha2, regexp_replace, round as spark_round, when
)

# ---- Load config from .env ----
load_dotenv()

CSV_PATH = os.getenv("CSV_PATH", "data/Sample - Superstore.csv")
JDBC_URL = os.getenv("JDBC_URL")
DB_USER = os.getenv("DB_USER")  # required
DB_PASS = os.getenv("DB_PASS")  # required
PG_JDBC_JAR = os.getenv(
    "PG_JDBC_JAR",
    "/Users/sankarshnellutla/Desktop/Vscode/Projects/superstore/sql/lib/postgresql-42.7.8.jar"
)

# target staging table (schema-qualified)
TARGET_TABLE = "stg.stg_superstore_raw"

# Validate minimal env
if not JDBC_URL:
    raise RuntimeError("JDBC_URL is required in .env; e.g. jdbc:postgresql://localhost:5433/dbname")
if not DB_USER or not DB_PASS:
    raise RuntimeError("DB_USER and DB_PASS are required in .env")
if not os.path.exists(PG_JDBC_JAR):
    raise FileNotFoundError(f"Postgres JDBC jar not found at {PG_JDBC_JAR}. Set PG_JDBC_JAR in .env or place jar there.")


# ---- Helper: Spark with JDBC jar on classpath ----
def create_spark_with_jdbc(jar_path, app_name="superstore-extract-jdbc"):
    """
    Build and return a SparkSession configured with the Postgres JDBC jar.
    We add the jar to spark.jars and driver/executor classpath so Spark can load the JDBC driver.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", jar_path)
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        .getOrCreate()
    )
    return spark


# ---- Main ETL extract routine ----
def run():
    spark = None
    tmp_path = None

    try:
        # 1) CSV existence check
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"CSV not found at: {CSV_PATH}. Put dataset in data/ or set CSV_PATH in .env")

        # 2) start Spark with JDBC jar on classpath
        spark = create_spark_with_jdbc(PG_JDBC_JAR)
        print("[EXTRACT] Spark session started with JDBC jar.")

        # 3) create UTF-8 temp copy to avoid encoding issues
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        tmp_path = tmp.name
        tmp.close()
        with open(CSV_PATH, "r", encoding="windows-1252", errors="replace") as fin, open(tmp_path, "w", encoding="utf-8") as fout:
            shutil.copyfileobj(fin, fout)
        print("[EXTRACT] Created UTF-8 temp CSV:", tmp_path)

        # 4) read CSV into Spark (strings only: inferSchema=False)
        sdf = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .csv(tmp_path)
        )
        row_count = sdf.count()
        print(f"[EXTRACT] CSV loaded into Spark DataFrame. Rows: {row_count}")

        # 5) trim whitespace (defensive)
        for c in sdf.columns:
            sdf = sdf.withColumn(c, trim(col(c)))
        print("[EXTRACT] Trimmed whitespace from all columns.")

        # 6) rename CSV headers to staging expected names (only if present)
        rename_map = {
            'Row ID':'row_id','Order ID':'order_key','Order Date':'order_date','Ship Date':'ship_date',
            'Ship Mode':'ship_mode','Customer ID':'customer_key','Customer Name':'customer_name',
            'Segment':'segment','Country':'country','City':'city','State':'state','Postal Code':'postal_code',
            'Region':'region','Product ID':'product_key','Category':'category','Sub-Category':'sub_category',
            'Product Name':'product_name','Sales':'sales','Quantity':'quantity','Discount':'discount','Profit':'profit'
        }
        for old, new in rename_map.items():
            if old in sdf.columns:
                sdf = sdf.withColumnRenamed(old, new)
        print("[EXTRACT] Renamed CSV columns to staging names where applicable.")

        # 7) ensure expected columns exist, to keep row_hash deterministic
        expected_cols = [
            'row_id','order_key','order_date','ship_date','ship_mode','customer_key','customer_name',
            'segment','country','city','state','postal_code','region','product_key','category',
            'sub_category','product_name','sales','quantity','discount','profit'
        ]
        for c in expected_cols:
            if c not in sdf.columns:
                sdf = sdf.withColumn(c, lit(None).cast("string"))

        # 8) sanitize numeric fields in Spark
        #    - remove currency symbol and thousands separators using regexp_replace
        #    - cast to double for numeric ops
        clean = (
            sdf
            .withColumn("sales_clean", regexp_replace(col("sales"), r'[$,]', ''))
            .withColumn("sales_clean", col("sales_clean").cast("double"))
            .withColumn("profit_clean", regexp_replace(col("profit"), r'[$,]', ''))
            .withColumn("profit_clean", col("profit_clean").cast("double"))
            .withColumn("discount_clean", regexp_replace(col("discount"), r'[$,]', ''))
            .withColumn("discount_clean", col("discount_clean").cast("double"))
            .withColumn("quantity_clean", col("quantity").cast("double"))
        )

        # normalize discount: >1 -> treat as percent (20 => 0.20); round to 2 decimals (matches numeric(3,2))
        clean = clean.withColumn(
            "discount_norm",
            spark_round(
                when(col("discount_clean").isNotNull() & (col("discount_clean") > 1),
                     col("discount_clean") / 100.0)
                .otherwise(col("discount_clean")),
                2
            )
        )

        # round sales/profit to 4 decimals (fits numeric(12,4) chosen)
        clean = clean.withColumn("sales_final", spark_round(col("sales_clean"), 4))
        clean = clean.withColumn("profit_final", spark_round(col("profit_clean"), 4))

        # convert quantity to integer (long) where possible (nulls remain null)
        clean = clean.withColumn(
            "quantity_final",
            when(col("quantity_clean").isNotNull(), col("quantity_clean").cast("long")).otherwise(lit(None))
        )

        # 9) deterministic row_hash (sha2 over concatenated expected columns)
        concat_cols = [coalesce(col(c).cast("string"), lit("")) for c in expected_cols]
        clean = clean.withColumn("row_hash", sha2(concat_ws("|", *concat_cols), 256))

        # 10) select final columns in exact order expected by stg.stg_superstore_raw DDL
        final_list = []
        for c in expected_cols:
            if c in ("sales", "profit", "discount", "quantity"):
                continue  # use cleaned versions below
            final_list.append(col(c).alias(c))

        final_list.extend([
            col("sales_final").alias("sales"),
            col("quantity_final").alias("quantity"),
            col("discount_norm").alias("discount"),
            col("profit_final").alias("profit"),
            col("row_hash").alias("row_hash")
        ])

        final_sdf = clean.select(*final_list)
        print("[EXTRACT] Prepared final Spark DataFrame (cleaned, hashed). Columns:", final_sdf.columns)

        # 11) JDBC write: overwrite existing table rows by TRUNCATE, preserving schema
        #     Explanation: Spark .mode("overwrite") would normally drop+create table; with option("truncate","true")
        #     many JDBC drivers will perform a TRUNCATE instead (Postgres supports this). This yields:
        #       - idempotent load: repeated runs replace rows
        #       - table DDL remains intact (indexes, constraints created outside this pipeline remain valid)
        jdbc_options = {
            "url": JDBC_URL,
            "dbtable": TARGET_TABLE,
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver",
            "batchsize": "1000"
        }

        # Use mode 'overwrite' with truncate=true to clear data (preserve schema). If the table doesn't exist,
        # Spark will create it using the inferred schema (that should match your staging DDL if created beforehand).
        print(f"[EXTRACT][JDBC] Writing to {TARGET_TABLE} with overwrite+truncate (idempotent).")
        (final_sdf
         .write
         .format("jdbc")
         .options(**jdbc_options)
         .option("truncate", "true")   # instructs Spark to use TRUNCATE (if supported) instead of drop/create
         .mode("overwrite")            # overwrite semantics
         .save()
         )
        print("[EXTRACT][JDBC] JDBC write finished (overwrite -> truncate applied).")

    except Exception as exc:
        # Bubble up but print a clear tag first
        print("[EXTRACT][ERROR]", exc)
        raise

    finally:
        # cleanup: remove temp CSV if created
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
                print("[EXTRACT] Removed temporary CSV:", tmp_path)
        except Exception:
            pass

        # Stop Spark session to avoid leaving JVM running
        try:
            if spark is not None:
                spark.stop()
                print("[EXTRACT] Spark stopped.")
        except Exception as e:
            print("[EXTRACT][WARN] spark.stop failed:", e)


if __name__ == "__main__":
    run()