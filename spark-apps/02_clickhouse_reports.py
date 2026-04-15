"""
Создание 6 витрин-отчётов в ClickHouse на основе модели "звезда" из PostgreSQL:
  1. product_sales_mart   - витрина продаж по продуктам
  2. customer_sales_mart  - витрина продаж по клиентам
  3. time_sales_mart      - витрина продаж по времени
  4. store_sales_mart     - витрина продаж по магазинам
  5. supplier_sales_mart  - витрина продаж по поставщикам
  6. quality_mart         - витрина качества продукции

Таблицы в ClickHouse уже созданы скриптом /docker-entrypoint-initdb.d/01_ddl.sql,
Spark использует mode=overwrite + truncate=true - сохраняет схему, очищает строки.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, concat_ws, coalesce, lit
)

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/reports"
CH_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "isolationLevel": "NONE",
}

spark = (SparkSession.builder
         .appName("clickhouse_reports")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

fact         = spark.read.jdbc(PG_URL, "fact_sales",   properties=PG_PROPS)
dim_customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
dim_product  = spark.read.jdbc(PG_URL, "dim_product",  properties=PG_PROPS)
dim_store    = spark.read.jdbc(PG_URL, "dim_store",    properties=PG_PROPS)
dim_supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
dim_date     = spark.read.jdbc(PG_URL, "dim_date",     properties=PG_PROPS)

# Стандартный JDBC-dialect Spark для неизвестных БД при mode=overwrite
# выполняет DROP+CREATE, и CREATE TABLE без ORDER BY для MergeTree падает.
# Поэтому делаем TRUNCATE руками через JDBC и используем mode=append.
# Стандартный JDBC-dialect Spark для неизвестных БД при mode=overwrite делает
# DROP+CREATE, и CREATE TABLE без ORDER BY для MergeTree падает. Поэтому для CH
# чистим таблицы через HTTP-интерфейс (порт 8123), а Spark просто append-ит.
import urllib.request

def ch_truncate(table):
    q = f"TRUNCATE TABLE reports.{table}"
    req = urllib.request.Request(
        "http://clickhouse:8123/?user=spark&password=spark",
        data=q.encode("utf-8"),
        method="POST",
    )
    urllib.request.urlopen(req).read()

def ch_write(df, table):
    ch_truncate(table)
    (df.write
       .mode("append")
       .option("isolationLevel", "NONE")
       .option("numPartitions", "1")
       .jdbc(CH_URL, table, properties=CH_PROPS))
    print(f"ClickHouse.reports.{table}: {df.count()} строк")

# 1. Витрина продаж по продуктам
product_mart = (fact.groupBy("product_id").agg(
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_price").alias("total_revenue"),
        count("sale_id").alias("order_count"),
    )
    .join(dim_product, "product_id", "left")
    .select(
        col("product_id").cast("long"),
        col("name").alias("product_name"),
        col("category"),
        col("total_quantity").cast("long"),
        col("total_revenue").cast("decimal(18,2)"),
        col("rating").cast("double").alias("avg_rating"),
        col("reviews").cast("long").alias("review_count"),
        col("order_count").cast("long"),
    ))

# 2. Витрина продаж по клиентам
customer_mart = (fact.groupBy("customer_id").agg(
        spark_sum("total_price").alias("total_spent"),
        count("sale_id").alias("order_count"),
        avg("total_price").alias("avg_check"),
    )
    .join(dim_customer, "customer_id", "left")
    .select(
        col("customer_id").cast("long"),
        concat_ws(" ", coalesce(col("first_name"), lit("")), coalesce(col("last_name"), lit(""))).alias("customer_name"),
        col("country"),
        col("total_spent").cast("decimal(18,2)"),
        col("order_count").cast("long"),
        col("avg_check").cast("decimal(18,2)"),
    ))

# 3. Витрина продаж по времени
time_mart = (fact.join(dim_date, "date_id", "left")
    .groupBy("year", "month").agg(
        spark_sum("total_price").alias("total_revenue"),
        count("sale_id").alias("order_count"),
        avg("total_price").alias("avg_order_size"),
    )
    .select(
        col("year").cast("int"),
        col("month").cast("int"),
        col("total_revenue").cast("decimal(18,2)"),
        col("order_count").cast("long"),
        col("avg_order_size").cast("decimal(18,2)"),
    ))

# 4. Витрина продаж по магазинам
store_mart = (fact.groupBy("store_id").agg(
        spark_sum("total_price").alias("total_revenue"),
        count("sale_id").alias("order_count"),
        avg("total_price").alias("avg_check"),
    )
    .join(dim_store, "store_id", "left")
    .select(
        col("store_id").cast("long"),
        col("name").alias("store_name"),
        col("city").alias("store_city"),
        col("country").alias("store_country"),
        col("total_revenue").cast("decimal(18,2)"),
        col("order_count").cast("long"),
        col("avg_check").cast("decimal(18,2)"),
    ))

# 5. Витрина продаж по поставщикам
fact_with_price = fact.join(dim_product.select(col("product_id"), col("price")), "product_id", "left")
supplier_mart = (fact_with_price.groupBy("supplier_id").agg(
        spark_sum("total_price").alias("total_revenue"),
        avg("price").alias("avg_product_price"),
        count("sale_id").alias("order_count"),
    )
    .join(dim_supplier, "supplier_id", "left")
    .select(
        col("supplier_id").cast("long"),
        col("name").alias("supplier_name"),
        col("country").alias("supplier_country"),
        col("total_revenue").cast("decimal(18,2)"),
        col("avg_product_price").cast("decimal(18,2)"),
        col("order_count").cast("long"),
    ))

# 6. Витрина качества продукции
quality_mart = (fact.groupBy("product_id").agg(
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_price").alias("total_revenue"),
    )
    .join(dim_product, "product_id", "left")
    .select(
        col("product_id").cast("long"),
        col("name").alias("product_name"),
        col("rating").cast("double"),
        col("reviews").cast("long").alias("review_count"),
        col("total_quantity").cast("long"),
        col("total_revenue").cast("decimal(18,2)"),
    ))

ch_write(product_mart,  "product_sales_mart")
ch_write(customer_mart, "customer_sales_mart")
ch_write(time_mart,     "time_sales_mart")
ch_write(store_mart,    "store_sales_mart")
ch_write(supplier_mart, "supplier_sales_mart")
ch_write(quality_mart,  "quality_mart")

print("Все 6 витрин записаны в ClickHouse (database=reports)")
spark.stop()
