-- create staging schema (safe to rerun)
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION CURRENT_USER;

-- raw staging: one-to-one with CSV columns (raw dates kept as text)
CREATE TABLE IF NOT EXISTS stg.stg_superstore_raw (
  row_id        varchar(64),
  order_key     varchar(20),
  order_date    varchar(50),
  ship_date     varchar(50),
  ship_mode     varchar(40),
  customer_key  varchar(12),
  customer_name varchar(200),
  segment       varchar(30),
  country       varchar(50),
  city          varchar(100),
  state         varchar(100),
  postal_code   varchar(20),
  region        varchar(50),
  product_key   varchar(30),
  category      varchar(50),
  sub_category  varchar(50),
  product_name  text,
  sales         numeric,
  quantity      int,
  discount      numeric,
  profit        numeric,
  loaded_at     timestamptz DEFAULT now(),
  row_hash      varchar(64)
);

-- dedupe / transform staging (typed / narrow shapes)
CREATE TABLE IF NOT EXISTS stg.stg_dim_location (
  city          varchar(50),
  state         varchar(50),
  postal_code   varchar(20),
  region        varchar(20)
);

CREATE TABLE IF NOT EXISTS stg.stg_dim_product (
  product_key   varchar(20),
  product_name  text,
  category      varchar(30),
  sub_category  varchar(40)
);

CREATE TABLE IF NOT EXISTS stg.stg_dim_customer (
  customer_key  varchar(12),
  customer_name varchar(100),
  segment       varchar(30),
  city          varchar(50),
  state         varchar(50),
  postal_code   varchar(20),
  region        varchar(20)
);

CREATE TABLE IF NOT EXISTS stg.stg_orders (
  order_key     varchar(20),
  order_date    date,
  ship_date     date,
  ship_mode     varchar(40),
  city          varchar(50),
  state         varchar(50),
  postal_code   varchar(20),
  region        varchar(20)
);

CREATE TABLE IF NOT EXISTS stg.stg_sales_lines (
  order_key     varchar(20),
  product_key   varchar(20),
  customer_key  varchar(12),
  quantity      smallint,
  sales         numeric(12,4),
  discount      numeric(3,2),
  profit        numeric(12,4)
);