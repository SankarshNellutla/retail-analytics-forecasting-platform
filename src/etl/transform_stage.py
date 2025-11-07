"""
transform_stage.py

Purpose:
 - Read raw staging rows from stg.stg_superstore_raw (written by extract.py)
 - Narrow & type the raw data into typed staging tables:
     - stg_dim_location      (city, state, postal_code, region)
     - stg_dim_product       (product_key, product_name, category, sub_category)
     - stg_dim_customer      (customer_key, customer_name, segment, city, state, postal_code, region)
     - stg_orders            (order_key, order_date, ship_date, ship_mode, city, state, postal_code, region)
     - stg_sales_lines       (order_key, product_key, customer_key, quantity, sales, discount, profit)
 - Use PySpark for all transforms (vectorized, deterministic), write back to Postgres using JDBC.
 - Idempotent: overwrite (truncate) typed staging tables on each run to keep staging predictable.
 - Honor numeric precisions: sales/profit -> 4 decimal places; discount -> 2 decimal places (0..1).
 - All DB connectivity controlled by .env:
     JDBC_URL, DB_USER, DB_PASS, PG_JDBC_JAR
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    trim, col, coalesce, lit, regexp_replace, round as spark_round,
    when, to_date
)

# Load .env
load_dotenv()

# Config (must exist in .env)
JDBC_URL = os.getenv("JDBC_URL")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
PG_JDBC_JAR = os.getenv("PG_JDBC_JAR", "/Users/sankarshnellutla/Desktop/Vscode/Projects/superstore/sql/lib/postgresql-42.7.8.jar")

# Source and target table names (schema-qualified)
RAW_TABLE = "stg.stg_superstore_raw"
TBL_DIM_LOCATION = "stg.stg_dim_location"
TBL_DIM_PRODUCT = "stg.stg_dim_product"
TBL_DIM_CUSTOMER = "stg.stg_dim_customer"
TBL_ORDERS = "stg.stg_orders"
TBL_SALES_LINES = "stg.stg_sales_lines"

# Basic validation
if not JDBC_URL:
    raise RuntimeError("JDBC_URL must be set in .env (e.g. jdbc:postgresql://host:5433/db)")
if not DB_USER or not DB_PASS:
    raise RuntimeError("DB_USER and DB_PASS must be set in .env")
if not os.path.exists(PG_JDBC_JAR):
    raise FileNotFoundError(f"Postgres JDBC jar not found at {PG_JDBC_JAR} (set PG_JDBC_JAR in .env)")

def create_spark_with_jdbc(jar_path, app_name="superstore-transform-stage"):
    """
    Create a SparkSession with the Postgres JDBC driver on classpath.
    We configure driver jar for both driver and executors.
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

def read_raw_staging(spark):
    """
    Read the raw staging table (stg.stg_superstore_raw) using Spark JDBC.
    We rely on extract.py to have persisted cleaned numeric columns already (but we do defensive casting).
    """
    print(f"[TRANSFORM] Reading raw staging table {RAW_TABLE} via JDBC...")
    raw_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", RAW_TABLE)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    print(f"[TRANSFORM] Read {raw_df.count()} rows from raw staging.")
    return raw_df

def sanitize_and_prepare(raw_df):
    """
    Core PySpark cleaning and typing logic.
    Inputs: raw_df (as read from stg.stg_superstore_raw) with columns like:
      row_id, order_key, order_date, ship_date, ship_mode, customer_key, customer_name,
      segment, country, city, state, postal_code, region, product_key, category, sub_category,
      product_name, sales, quantity, discount, profit, row_hash

    Outputs: typed DataFrames for each typed staging target.
    """
    # Defensive: trim all string columns
    for c in raw_df.columns:
        raw_df = raw_df.withColumn(c, trim(col(c)))

    # Ensure expected columns exist (defensive) - add as nulls if missing
    expected_cols = [
        'row_id','order_key','order_date','ship_date','ship_mode','customer_key','customer_name',
        'segment','country','city','state','postal_code','region','product_key','category',
        'sub_category','product_name','sales','quantity','discount','profit','row_hash'
    ]
    for c in expected_cols:
        if c not in raw_df.columns:
            raw_df = raw_df.withColumn(c, lit(None).cast("string"))

    # ---------- Numeric sanitization (defensive) ----------
    # Remove $ and commas first, then cast only if the cleaned text looks numeric (rlike). This avoids
    # CAST errors when malformed strings land in numeric columns.
    clean = raw_df \
        .withColumn("sales_clean_raw", regexp_replace(col("sales"), r'[$,]', '')) \
        .withColumn("profit_clean_raw", regexp_replace(col("profit"), r'[$,]', '')) \
        .withColumn("discount_clean_raw", regexp_replace(col("discount"), r'[$,]', '')) \
        .withColumn("quantity_clean_raw", col("quantity"))

    # If the cleaned text matches a simple numeric regex, cast; else NULL.
    numeric_regex = r'^[+-]?\d+(\.\d+)?$'

    clean = clean \
        .withColumn("sales_num", when(col("sales_clean_raw").rlike(numeric_regex), col("sales_clean_raw").cast("double")).otherwise(lit(None))) \
        .withColumn("profit_num", when(col("profit_clean_raw").rlike(numeric_regex), col("profit_clean_raw").cast("double")).otherwise(lit(None))) \
        .withColumn("discount_num", when(col("discount_clean_raw").rlike(numeric_regex), col("discount_clean_raw").cast("double")).otherwise(lit(None))) \
        .withColumn("quantity_num", when(col("quantity_clean_raw").rlike(r'^[+-]?\d+(\.\d+)?$'), col("quantity_clean_raw").cast("double")).otherwise(lit(None)))

    # Normalize discount:
    # If discount_num > 1, treat as percent (e.g., 20 -> 0.20). Round to 2 decimals (fits numeric(3,2)).
    clean = clean.withColumn(
        "discount_norm",
        spark_round(
            when(col("discount_num").isNotNull() & (col("discount_num") > 1),
                 col("discount_num") / 100.0)
            .otherwise(col("discount_num")),
            2
        )
    )

    # Round sales/profit to 4 decimals (we chose numeric(12,4) earlier)
    clean = clean.withColumn("sales_final", spark_round(col("sales_num"), 4))
    clean = clean.withColumn("profit_final", spark_round(col("profit_num"), 4))

    # Convert quantity to integer (long) where possible; keep nulls as null
    clean = clean.withColumn("quantity_final",
                             when(col("quantity_num").isNotNull(), col("quantity_num").cast("long")).otherwise(lit(None))
                             )

    # ---------- Date parsing (try two formats) ----------
    # first try M/d/yyyy (handles 1/2/2018 or 01/02/2018), then fallback to ISO yyyy-MM-dd
    clean = clean.withColumn("order_date_parsed",
                             coalesce(
                                 to_date(col("order_date"), "M/d/yyyy"),
                                 to_date(col("order_date"), "yyyy-MM-dd")
                             ))
    clean = clean.withColumn("ship_date_parsed",
                             coalesce(
                                 to_date(col("ship_date"), "M/d/yyyy"),
                                 to_date(col("ship_date"), "yyyy-MM-dd")
                             ))

    # ---------- Build typed staging tables ----------

    # 1) stg_dim_location: unique combinations of city/state/postal_code/region
    stg_dim_location = (
        clean.select("city", "state", "postal_code", "region")
        .dropDuplicates()
    )

    # 2) stg_dim_product: unique product_key rows
    stg_dim_product = (
        clean.select("product_key", "product_name", "category", "sub_category")
        .dropDuplicates(["product_key"])
    )

    # 3) stg_dim_customer: unique customer_key with location attributes
    stg_dim_customer = (
        clean.select("customer_key", "customer_name", "segment", "city", "state", "postal_code", "region")
        .dropDuplicates(["customer_key"])
    )

    # 4) stg_orders: order header per order_key (keep shipping location columns for later mapping)
    stg_orders = (
        clean.select(
            col("order_key"),
            col("order_date_parsed").alias("order_date"),
            col("ship_date_parsed").alias("ship_date"),
            col("ship_mode"),
            col("city"),
            col("state"),
            col("postal_code"),
            col("region")
        )
        .dropDuplicates(["order_key"])
    )

    # 5) stg_sales_lines: the fact grain (order-line)
    stg_sales_lines = (
        clean.select(
            "order_key",
            "product_key",
            "customer_key",
            col("quantity_final").alias("quantity"),
            col("sales_final").alias("sales"),
            col("discount_norm").alias("discount"),
            col("profit_final").alias("profit")
        )
    )

    # Final casting/rounding checks (explicit types)
    stg_sales_lines = stg_sales_lines \
        .withColumn("sales", col("sales").cast("double")) \
        .withColumn("profit", col("profit").cast("double")) \
        .withColumn("discount", col("discount").cast("double")) \
        .withColumn("quantity", col("quantity").cast("int"))

    return {
        "stg_dim_location": stg_dim_location,
        "stg_dim_product": stg_dim_product,
        "stg_dim_customer": stg_dim_customer,
        "stg_orders": stg_orders,
        "stg_sales_lines": stg_sales_lines
    }

def write_table(spark_df, target_table):
    """
    Write a Spark DataFrame to Postgres via JDBC using overwrite+truncate semantics.
    This truncates rows while preserving table DDL (indexes/constraints), making the operation idempotent.
    """
    print(f"[TRANSFORM][JDBC] Writing {target_table} ({spark_df.count()} rows) ...")
    # JDBC options
    options = {
        "url": JDBC_URL,
        "dbtable": target_table,
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
        "batchsize": "1000"
    }
    # Overwrite with truncate to preserve schema
    (spark_df
     .write
     .format("jdbc")
     .options(**options)
     .option("truncate", "true")  # instruct to perform TRUNCATE of existing table (Postgres)
     .mode("overwrite")
     .save()
     )
    print(f"[TRANSFORM][JDBC] Finished writing {target_table}.")

def run():
    spark = None
    try:
        # 1) Create Spark session with JDBC jar
        spark = create_spark_with_jdbc(PG_JDBC_JAR)
        print("[TRANSFORM] Spark session started for transform_stage")

        # 2) Read raw staging using JDBC (no pandas)
        raw_df = read_raw_staging(spark)

        # Early exit if empty
        if raw_df.rdd.isEmpty():
            print("[TRANSFORM] No rows in raw staging, exiting.")
            return

        # 3) Sanitize / prepare typed staging DataFrames
        typed = sanitize_and_prepare(raw_df)

        # 4) Write each typed staging table back to Postgres via JDBC (overwrite/truncate)
        write_table(typed["stg_dim_location"], TBL_DIM_LOCATION)
        write_table(typed["stg_dim_product"], TBL_DIM_PRODUCT)
        write_table(typed["stg_dim_customer"], TBL_DIM_CUSTOMER)
        write_table(typed["stg_orders"], TBL_ORDERS)
        write_table(typed["stg_sales_lines"], TBL_SALES_LINES)

        # 5) Print final counts from Spark (fast, no roundtrip)
        print("[TRANSFORM] Final typed staging counts (queried from Spark DataFrames):")
        print(f"  stg_dim_location : {typed['stg_dim_location'].count()}")
        print(f"  stg_dim_product  : {typed['stg_dim_product'].count()}")
        print(f"  stg_dim_customer : {typed['stg_dim_customer'].count()}")
        print(f"  stg_orders       : {typed['stg_orders'].count()}")
        print(f"  stg_sales_lines  : {typed['stg_sales_lines'].count()}")

        print("[TRANSFORM] transform_stage completed successfully.")

    except Exception as e:
        print("[TRANSFORM][ERROR]", e)
        raise

    finally:
        # Ensure Spark session is stopped cleanly to avoid stale JVMs
        try:
            if spark is not None:
                spark.stop()
                print("[TRANSFORM] Spark stopped.")
        except Exception as ex:
            print("[TRANSFORM][WARN] spark.stop failed:", ex)


if __name__ == "__main__":
    run()