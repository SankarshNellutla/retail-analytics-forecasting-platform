"""
transform_load_final.py

Purpose:
 - Read typed staging tables (stg.*) produced by transform_stage.py.
 - Perform minimal final transformations and mappings:
     * compute unit_price, normalize discount, parse dates -> year/month
 - Upsert dimensions (dim_date, dim_location, dim_product, dim_customer, dim_order)
 - Load fact_sales idempotently (delete existing facts for affected orders -> insert)
 - Strictly enforce NOT NULL constraints declared in final DDL:
     dim_order.order_date_id/ship_date_id must be present (we skip orders missing both)
     fact_sales: require order_id, product_id, customer_id, location_id, sales, unit_price, discount
"""

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL required in .env")

# Fully-qualified typed staging tables created by transform_stage.py
STG_DIM_LOCATION = "stg.stg_dim_location"
STG_DIM_PRODUCT = "stg.stg_dim_product"
STG_DIM_CUSTOMER = "stg.stg_dim_customer"
STG_ORDERS = "stg.stg_orders"
STG_SALES = "stg.stg_sales_lines"

# ---------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------
def read_table_sql(engine, fq_table):
    """Read a schema-qualified table (schema.table) into a pandas DataFrame."""
    try:
        schema, table = fq_table.split(".", 1)
    except Exception:
        raise ValueError(f"fq_table must be 'schema.table' got: {fq_table}")
    try:
        df = pd.read_sql_table(table, con=engine, schema=schema)
        return df
    except Exception as e:
        print(f"[WARN] Failed to read {fq_table}: {e} — returning empty DataFrame")
        return pd.DataFrame()

def safe_numeric_series(s):
    """Convert a Series to floats, stripping $ and commas; coerce invalid -> NaN."""
    if s is None:
        return pd.Series(dtype="float64")
    return pd.to_numeric(s.astype(str).str.replace(r'[$,]', '', regex=True).replace({"": None}), errors="coerce")

def normalize_discount(s):
    """
    Normalize discount values to fraction 0..1 and round to 2 decimals.
    Accepts inputs like 0.2 or 20 and converts 20 -> 0.20.
    """
    if s is None:
        return pd.Series(dtype="float64")
    series = safe_numeric_series(s)
    def norm_val(x):
        if pd.isna(x):
            return None
        v = float(x)
        if v > 1:
            v = v / 100.0
        v = max(0.0, min(1.0, v))
        return round(v, 2)
    return series.apply(lambda v: norm_val(v) if pd.notna(v) else None)

def compute_unit_price(sales_s, qty_s):
    """Compute unit_price = sales/quantity rounded to 4 decimals. Return None when invalid."""
    out = []
    for s, q in zip(sales_s, qty_s):
        try:
            if pd.isna(s) or pd.isna(q) or int(q) == 0:
                out.append(None)
            else:
                up = float(s) / int(q)
                out.append(round(up, 4))
        except Exception:
            out.append(None)
    return pd.Series(out)

# ---------------------------------------------------------------------
# Upsert helpers (dimension tables)
# ---------------------------------------------------------------------
def upsert_dim_date(engine, dates):
    """
    Upsert list of date-like values into dim_date.
    IMPORTANT: day_of_week mapping uses Sunday=0 per DDL comment:
      dow = (python_weekday + 1) % 7
    """
    if not dates:
        return
    stmt = text("""
    INSERT INTO dim_date (full_date, year, quarter, month, day, day_of_week, is_weekend)
    VALUES (:full_date, :year, :quarter, :month, :day, :dow, :is_weekend)
    ON CONFLICT (full_date) DO UPDATE
      SET year = EXCLUDED.year,
          quarter = EXCLUDED.quarter,
          month = EXCLUDED.month,
          day = EXCLUDED.day,
          day_of_week = EXCLUDED.day_of_week,
          is_weekend = EXCLUDED.is_weekend
    """)
    # Normalize unique dates (as date objects)
    unique_dates = sorted({pd.to_datetime(d).date() for d in dates if pd.notna(d)})
    if not unique_dates:
        return
    with engine.begin() as conn:
        for d in unique_dates:
            year = d.year
            month = d.month
            quarter = ((month - 1) // 3) + 1
            day = d.day
            # Python weekday(): Monday=0..Sunday=6
            # DDL expects Sunday=0 -> map accordingly:
            dow = (d.weekday() + 1) % 7
            is_weekend = dow == 0 or dow == 6  # Sunday(0) or Saturday(6)
            conn.execute(stmt, {"full_date": d, "year": year, "quarter": quarter, "month": month,
                                "day": day, "dow": dow, "is_weekend": is_weekend})

def upsert_dim_location(engine, df_loc):
    """Upsert rows into dim_location using unique key (city,state,postal_code,region)."""
    if df_loc.empty:
        return
    df = df_loc.drop_duplicates(subset=["city","state","postal_code","region"])
    stmt = text("""
    INSERT INTO dim_location (city, state, postal_code, region)
    VALUES (:city, :state, :postal_code, :region)
    ON CONFLICT (city, state, postal_code, region) DO UPDATE
      SET city = EXCLUDED.city
    """)
    with engine.begin() as conn:
        for _, r in df.iterrows():
            city = None if pd.isna(r.get("city")) else r.get("city")
            state = None if pd.isna(r.get("state")) else r.get("state")
            postal_code = None if pd.isna(r.get("postal_code")) else r.get("postal_code")
            region = None if pd.isna(r.get("region")) else r.get("region")
            conn.execute(stmt, {"city": city, "state": state,
                                "postal_code": postal_code, "region": region})

def upsert_dim_product(engine, df_prod):
    """Upsert dim_product using product_key unique constraint."""
    if df_prod.empty:
        return
    df = df_prod.drop_duplicates(subset=["product_key"])
    stmt = text("""
    INSERT INTO dim_product (product_key, product_name, category, sub_category)
    VALUES (:product_key, :product_name, :category, :sub_category)
    ON CONFLICT (product_key) DO UPDATE
      SET product_name = EXCLUDED.product_name,
          category = EXCLUDED.category,
          sub_category = EXCLUDED.sub_category
    """)
    with engine.begin() as conn:
        for _, r in df.iterrows():
            product_key = None if pd.isna(r.get("product_key")) else r.get("product_key")
            product_name = None if pd.isna(r.get("product_name")) else r.get("product_name")
            category = None if pd.isna(r.get("category")) else r.get("category")
            sub_category = None if pd.isna(r.get("sub_category")) else r.get("sub_category")
            conn.execute(stmt, {"product_key": product_key, "product_name": product_name,
                               "category": category, "sub_category": sub_category})

def upsert_dim_customer(engine, df_cust, loc_map_df):
    """
    Upsert dim_customer using customer_key unique.
    Map location_id via loc_map_df (city/state/postal_code/region -> location_id) if available.
    """
    if df_cust.empty:
        return
    df = df_cust.drop_duplicates(subset=["customer_key"])
    if not loc_map_df.empty:
        df = df.merge(loc_map_df, how="left", on=["city","state","postal_code","region"])
    else:
        df["location_id"] = None
    stmt = text("""
    INSERT INTO dim_customer (customer_key, customer_name, segment, location_id)
    VALUES (:customer_key, :customer_name, :segment, :location_id)
    ON CONFLICT (customer_key) DO UPDATE
      SET customer_name = EXCLUDED.customer_name,
          segment = EXCLUDED.segment,
          location_id = COALESCE(EXCLUDED.location_id, dim_customer.location_id)
    """)
    with engine.begin() as conn:
        for _, r in df.iterrows():
            customer_key = None if pd.isna(r.get("customer_key")) else r.get("customer_key")
            customer_name = None if pd.isna(r.get("customer_name")) else r.get("customer_name")
            segment = None if pd.isna(r.get("segment")) else r.get("segment")
            location_id = int(r.get("location_id")) if (r.get("location_id") is not None and pd.notna(r.get("location_id"))) else None
            conn.execute(stmt, {"customer_key": customer_key, "customer_name": customer_name,
                               "segment": segment, "location_id": location_id})

def upsert_dim_order(engine, df_orders, date_map_df, loc_map_df):
    """
    Upsert dim_order using order_key unique.
    Important: dim_order.order_date_id and ship_date_id are NOT NULL in DDL.
    Therefore we MUST skip orders where mapping to dim_date fails (or the input date is missing).
    We log how many orders are skipped due to missing parsed dates or missing date mapping.
    """
    if df_orders.empty:
        return

    df = df_orders.drop_duplicates(subset=["order_key"]).copy()
    # ensure order/ship date are date type
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce").dt.date

    # Map full_date -> date_id
    if not date_map_df.empty:
        date_map = date_map_df.copy()
        date_map["full_date"] = pd.to_datetime(date_map["full_date"]).dt.date
        date_map = date_map.rename(columns={"full_date": "mapped_full_date", "date_id": "mapped_date_id"})
        # left join for order_date
        df = df.merge(date_map, left_on="order_date", right_on="mapped_full_date", how="left")
        df = df.rename(columns={"mapped_date_id": "order_date_id"}).drop(columns=["mapped_full_date"], errors="ignore")
        # left join for ship_date (use a new map)
        date_map2 = date_map.rename(columns={"mapped_full_date":"mapped_full_date2","mapped_date_id":"mapped_date_id2"})
        df = df.merge(date_map2, left_on="ship_date", right_on="mapped_full_date2", how="left")
        df = df.rename(columns={"mapped_date_id2":"ship_date_id"}).drop(columns=["mapped_full_date2"], errors="ignore")
    else:
        df["order_date_id"] = None
        df["ship_date_id"] = None

    # Map shipping location -> location_id (if loc_map_df provided)
    if not loc_map_df.empty:
        df = df.merge(loc_map_df, how="left", on=["city","state","postal_code","region"])
    else:
        df["location_id"] = None

    # Now filter: drop orders missing order_date_id or ship_date_id
    mask_missing_dates = df["order_date_id"].isnull() | df["ship_date_id"].isnull()
    n_missing = int(mask_missing_dates.sum())
    if n_missing > 0:
        print(f"[WARN] Skipping {n_missing} orders due to missing order_date_id or ship_date_id (NOT NULL in DDL). Sample:")
        print(df.loc[mask_missing_dates, ["order_key","order_date","ship_date"]].head(10))
        df = df.loc[~mask_missing_dates].copy()  # keep only valid rows

    if df.empty:
        print("[INFO] No valid orders to upsert after dropping rows with missing date mappings.")
        return

    stmt = text("""
    INSERT INTO dim_order (order_key, order_date_id, ship_date_id, ship_mode, location_id, order_year, order_month)
    VALUES (:order_key, :order_date_id, :ship_date_id, :ship_mode, :location_id, :order_year, :order_month)
    ON CONFLICT (order_key) DO UPDATE
      SET order_date_id = COALESCE(EXCLUDED.order_date_id, dim_order.order_date_id),
          ship_date_id  = COALESCE(EXCLUDED.ship_date_id, dim_order.ship_date_id),
          ship_mode     = EXCLUDED.ship_mode,
          location_id   = COALESCE(EXCLUDED.location_id, dim_order.location_id),
          order_year    = COALESCE(EXCLUDED.order_year, dim_order.order_year),
          order_month   = COALESCE(EXCLUDED.order_month, dim_order.order_month)
    """)
    with engine.begin() as conn:
        for _, r in df.iterrows():
            od = r.get("order_date")
            if pd.isna(od) or od is None:
                order_year = None
                order_month = None
            else:
                order_year = int(pd.to_datetime(od).year)
                order_month = int(pd.to_datetime(od).month)
            conn.execute(stmt, {"order_key": r.get("order_key"),
                                "order_date_id": int(r.get("order_date_id")) if pd.notna(r.get("order_date_id")) else None,
                                "ship_date_id": int(r.get("ship_date_id")) if pd.notna(r.get("ship_date_id")) else None,
                                "ship_mode": None if pd.isna(r.get("ship_mode")) else r.get("ship_mode"),
                                "location_id": int(r.get("location_id")) if (r.get("location_id") is not None and pd.notna(r.get("location_id"))) else None,
                                "order_year": order_year,
                                "order_month": order_month})

# ---------------------------------------------------------------------
# Load fact_sales (idempotent)
# ---------------------------------------------------------------------
def load_fact_sales(engine, df_sales, order_map_df, prod_map_df, cust_map_df):
    """
    Idempotent load for fact_sales:
     - Map natural keys -> surrogate IDs
     - Ensure required non-null fields (per DDL)
     - Delete existing facts for the affected order_ids
     - Insert new facts
    Required (DDL): order_id, product_id, customer_id, location_id, quantity, sales, unit_price, discount
    """
    if df_sales.empty:
        print("[LOAD] No sales data to load.")
        return

    df = df_sales.copy()

    # Map natural keys -> surrogate ids (merge mapping DFs)
    if not order_map_df.empty:
        df = df.merge(order_map_df, how="left", on="order_key")
    if not prod_map_df.empty:
        df = df.merge(prod_map_df, how="left", on="product_key")
    if not cust_map_df.empty:
        df = df.merge(cust_map_df, how="left", on="customer_key")

    # Normalize numeric fields
    df["sales"] = safe_numeric_series(df.get("sales"))
    df["profit"] = safe_numeric_series(df.get("profit"))
    df["discount"] = normalize_discount(df.get("discount"))
    df["quantity"] = pd.to_numeric(df.get("quantity"), errors="coerce")

    # Compute unit_price if missing
    df["unit_price"] = compute_unit_price(df["sales"], df["quantity"])

    # Fill discount default to 0.0 for missing (DDL expects NOT NULL)
    df["discount"] = df["discount"].fillna(0.0)

    # Drop rows that fail required conditions
    pre_count = len(df)
    cond_ok = (
        df["order_id"].notna()
        & df["product_id"].notna()
        & df["customer_id"].notna()
        & df["location_id"].notna()
        & df["sales"].notna()
        & df["quantity"].notna()
        & (df["quantity"].astype(float) > 0)
        & df["unit_price"].notna()
    )
    dropped = df.loc[~cond_ok]
    if not dropped.empty:
        print(f"[LOAD][WARN] Dropping {len(dropped)} sales rows that do not satisfy required fields (sample):")
        print(dropped.head(10))
    df = df.loc[cond_ok].copy()
    if df.empty:
        print("[LOAD] Nothing to insert after dropping invalid rows.")
        return

    # Round numeric columns to match schema
    df["sales"] = df["sales"].apply(lambda v: round(float(v), 4))
    df["profit"] = df["profit"].apply(lambda v: round(float(v), 4) if pd.notna(v) else None)
    df["unit_price"] = df["unit_price"].apply(lambda v: round(float(v), 4) if pd.notna(v) else None)
    df["discount"] = df["discount"].apply(lambda v: round(float(v), 2) if pd.notna(v) else 0.0)

    # Identify affected order_ids (surrogate) and delete existing facts for them (idempotence)
    order_ids = df["order_id"].astype(int).unique().tolist()
    with engine.begin() as conn:
        # Delete existing facts for affected orders
        conn.execute(text("DELETE FROM fact_sales WHERE order_id = ANY(:order_ids)"), {"order_ids": order_ids})

        # Bulk insert new fact rows
        insert_stmt = text("""
        INSERT INTO fact_sales (order_id, product_id, customer_id, location_id, quantity, sales, unit_price, discount, profit)
        VALUES (:order_id, :product_id, :customer_id, :location_id, :quantity, :sales, :unit_price, :discount, :profit)
        """)
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "order_id": int(r["order_id"]),
                "product_id": int(r["product_id"]),
                "customer_id": int(r["customer_id"]),
                "location_id": int(r["location_id"]),
                "quantity": int(r["quantity"]),
                "sales": float(r["sales"]),
                "unit_price": float(r["unit_price"]),
                "discount": float(r["discount"]),
                "profit": float(r["profit"]) if pd.notna(r.get("profit")) else None
            })
        if rows:
            conn.execute(insert_stmt, rows)
        print(f"[LOAD] Inserted {len(rows)} fact_sales rows for {len(order_ids)} orders (dropped {pre_count - len(df)} rows).")

# ---------------------------------------------------------------------
# Main flow: orchestrates reading staged tables, upserting dims, and loading facts
# ---------------------------------------------------------------------
def run():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    print("[START] Connected to DB")

    # Read typed staging tables (output of transform_stage.py)
    df_loc = read_table_sql(engine, STG_DIM_LOCATION)
    df_prod = read_table_sql(engine, STG_DIM_PRODUCT)
    df_cust = read_table_sql(engine, STG_DIM_CUSTOMER)
    df_orders = read_table_sql(engine, STG_ORDERS)
    df_sales = read_table_sql(engine, STG_SALES)

    print("[INFO] staging counts:", f"loc={len(df_loc)} prod={len(df_prod)} cust={len(df_cust)} orders={len(df_orders)} sales={len(df_sales)}")

    # Minimal final transforms:
    # normalize numeric columns in sales and compute unit_price
    if not df_sales.empty:
        df_sales["sales"] = safe_numeric_series(df_sales.get("sales"))
        df_sales["profit"] = safe_numeric_series(df_sales.get("profit"))
        df_sales["discount"] = normalize_discount(df_sales.get("discount"))
        df_sales["quantity"] = pd.to_numeric(df_sales.get("quantity"), errors="coerce")
        df_sales["unit_price"] = compute_unit_price(df_sales["sales"], df_sales["quantity"])

    # Parse orders dates and compute convenience year/month
    if not df_orders.empty:
        df_orders["order_date"] = pd.to_datetime(df_orders["order_date"], errors="coerce").dt.date
        df_orders["ship_date"] = pd.to_datetime(df_orders["ship_date"], errors="coerce").dt.date
        df_orders["order_year"] = df_orders["order_date"].apply(lambda d: int(d.year) if pd.notna(d) else None)
        df_orders["order_month"] = df_orders["order_date"].apply(lambda d: int(d.month) if pd.notna(d) else None)

    # 1) Upsert dimension data
    # dim_date from order_date & ship_date (must run BEFORE dim_order so we can map dates -> ids)
    dates = []
    if not df_orders.empty:
        dates += df_orders["order_date"].dropna().tolist()
        dates += df_orders["ship_date"].dropna().tolist()
    upsert_dim_date(engine, dates)
    print("[STEP] dim_date upsert done")

    # dim_location
    upsert_dim_location(engine, df_loc)
    print("[STEP] dim_location upsert done")

    # dim_product
    upsert_dim_product(engine, df_prod)
    print("[STEP] dim_product upsert done")

    # Refresh location mapping from DB (for customers and orders)
    loc_map_df = pd.read_sql("SELECT location_id, city, state, postal_code, region FROM dim_location", engine)

    # dim_customer (map to location_id)
    upsert_dim_customer(engine, df_cust, loc_map_df)
    print("[STEP] dim_customer upsert done")

    # Refresh product and customer maps
    prod_map_df = pd.read_sql("SELECT product_id, product_key FROM dim_product", engine)
    cust_map_df = pd.read_sql("SELECT customer_id, customer_key FROM dim_customer", engine)

    # dim_order (map dates to dim_date ids, location map)
    date_map_df = pd.read_sql("SELECT date_id, full_date FROM dim_date", engine)
    # Prepare loc map for joining by city/state/postal/region
    loc_map_for_join = pd.read_sql("SELECT location_id, city, state, postal_code, region FROM dim_location", engine)
    upsert_dim_order(engine, df_orders, date_map_df, loc_map_for_join)
    print("[STEP] dim_order upsert done")

    # Refresh order map for fact loading
    order_map_df = pd.read_sql("SELECT order_id, order_key, location_id FROM dim_order", engine)

    # 2) Load facts idempotently
    load_fact_sales(engine,
                    df_sales,
                    order_map_df.rename(columns={"order_key":"order_key","order_id":"order_id"}),
                    prod_map_df.rename(columns={"product_key":"product_key","product_id":"product_id"}),
                    cust_map_df.rename(columns={"customer_key":"customer_key","customer_id":"customer_id"}))

    engine.dispose()
    print("[DONE] transform_load_final completed successfully.")

if __name__ == "__main__":
    run()