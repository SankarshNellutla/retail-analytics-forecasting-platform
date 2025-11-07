-- DIMENSION: dim_date (calendar)

CREATE TABLE IF NOT EXISTS dim_date (
  date_id      int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  full_date    date NOT NULL UNIQUE,
  year         smallint NOT NULL CHECK (year BETWEEN 1900 AND 2100),
  quarter      smallint NOT NULL CHECK (quarter BETWEEN 1 AND 4),
  month        smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
  day          smallint NOT NULL CHECK (day BETWEEN 1 AND 31),
  day_of_week  smallint NOT NULL CHECK (day_of_week BETWEEN 0 AND 6), -- 0=Sunday
  is_weekend   boolean NOT NULL DEFAULT false,
  is_holiday   boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);

-- DIMENSION: dim_location
CREATE TABLE IF NOT EXISTS dim_location (
  location_id   int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  city          varchar(50),
  state         varchar(50),
  postal_code   varchar(20),
  region        varchar(20),
  UNIQUE (city, state, postal_code, region)
);

-- DIMENSION: dim_customer
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id   int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- internal surrogate
  customer_key  varchar(12) NOT NULL UNIQUE,                   -- natural CSV key (Customer ID)
  customer_name varchar(100) NOT NULL,
  segment       varchar(30) NOT NULL,
  location_id   int REFERENCES dim_location(location_id)
);
CREATE INDEX IF NOT EXISTS idx_dim_customer_key ON dim_customer(customer_key);

-- DIMENSION: dim_product
CREATE TABLE IF NOT EXISTS dim_product (
  product_id    int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- internal surrogate
  product_key   varchar(20) NOT NULL UNIQUE,                  -- natural CSV key (Product ID)
  product_name  text NOT NULL,
  category      varchar(30) NOT NULL,
  sub_category  varchar(40)
);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category);

-- ORDERS (order header)
CREATE TABLE IF NOT EXISTS dim_order (
  order_id      int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- internal surrogate
  order_key     varchar(20) NOT NULL UNIQUE,                   -- natural CSV key (Order ID)
  order_date_id int NOT NULL REFERENCES dim_date(date_id),     -- FK to dim_date
  ship_date_id  int NOT NULL REFERENCES dim_date(date_id),
  ship_mode     varchar(40) NOT NULL,
  location_id   int REFERENCES dim_location(location_id),     -- shipping location
  order_year    smallint,  -- filled by ETL (from order_date)
  order_month   smallint   -- filled by ETL (from order_date)
);
CREATE INDEX IF NOT EXISTS idx_dim_order_order_key ON dim_order(order_key);
CREATE INDEX IF NOT EXISTS idx_dim_order_year_month ON dim_order(order_year, order_month);

-- FACT: fact_sales (grain: one order-line)
CREATE TABLE IF NOT EXISTS fact_sales (
  sales_id      int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_id      int NOT NULL REFERENCES dim_order(order_id),           -- surrogate order FK
  product_id    int NOT NULL REFERENCES dim_product(product_id),       -- surrogate product FK
  customer_id   int NOT NULL REFERENCES dim_customer(customer_id),     -- surrogate customer FK
  location_id   int NOT NULL REFERENCES dim_location(location_id),     -- shipping location FK

  quantity      smallint NOT NULL CHECK (quantity > 0),
  sales         numeric(12,4) NOT NULL CHECK (sales >= 0),             -- dollars, 4 decimal places
  unit_price    numeric(12,4) NOT NULL,                                -- computed in ETL
  discount      numeric(3,2) NOT NULL CHECK (discount >= 0 AND discount <= 1),
  profit        numeric(12,4),

  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order ON fact_sales(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_location ON fact_sales(location_id);

-- HELPERS: views to map natural keys -> surrogate ids (handy in ETL)
CREATE OR REPLACE VIEW vw_customer_key_to_id AS SELECT customer_key, customer_id FROM dim_customer;
CREATE OR REPLACE VIEW vw_product_key_to_id  AS SELECT product_key,  product_id  FROM dim_product;
CREATE OR REPLACE VIEW vw_date_to_id         AS SELECT full_date,    date_id     FROM dim_date;
CREATE OR REPLACE VIEW vw_location_key_to_id AS SELECT city,state,postal_code,region,location_id FROM dim_location;