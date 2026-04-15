CREATE DATABASE IF NOT EXISTS reports;

CREATE TABLE IF NOT EXISTS reports.product_sales_mart (
    product_id Int64,
    product_name Nullable(String),
    category Nullable(String),
    total_quantity Int64,
    total_revenue Decimal(18, 2),
    avg_rating Nullable(Float64),
    review_count Nullable(Int64),
    order_count Int64
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE IF NOT EXISTS reports.customer_sales_mart (
    customer_id Int64,
    customer_name Nullable(String),
    country Nullable(String),
    total_spent Decimal(18, 2),
    order_count Int64,
    avg_check Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS reports.time_sales_mart (
    year Int32,
    month Int32,
    total_revenue Decimal(18, 2),
    order_count Int64,
    avg_order_size Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS reports.store_sales_mart (
    store_id Int64,
    store_name Nullable(String),
    store_city Nullable(String),
    store_country Nullable(String),
    total_revenue Decimal(18, 2),
    order_count Int64,
    avg_check Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY store_id;

CREATE TABLE IF NOT EXISTS reports.supplier_sales_mart (
    supplier_id Int64,
    supplier_name Nullable(String),
    supplier_country Nullable(String),
    total_revenue Decimal(18, 2),
    avg_product_price Decimal(18, 2),
    order_count Int64
) ENGINE = MergeTree() ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS reports.quality_mart (
    product_id Int64,
    product_name Nullable(String),
    rating Nullable(Float64),
    review_count Nullable(Int64),
    total_quantity Int64,
    total_revenue Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY product_id;
