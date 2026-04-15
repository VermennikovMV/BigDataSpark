"""
Трансформация данных из mock_data (PostgreSQL) в модель "звезда" в PostgreSQL.
Создаёт таблицы: dim_customer, dim_seller, dim_product, dim_store,
dim_supplier, dim_date, fact_sales.

Так как id клиентов/продавцов/продуктов пересекаются между 10 файлами,
суррогатные ключи генерируются через xxhash64 от естественных атрибутов
(email, имя/бренд, имя/город и т.п.), чтобы получить детерминированные
и уникальные идентификаторы.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, xxhash64, date_format, year, month, dayofmonth, quarter,
    row_number, monotonically_increasing_id
)
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

spark = (SparkSession.builder
         .appName("star_schema")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)

raw = (raw
    .withColumn("customer_key", xxhash64(col("customer_email")))
    .withColumn("seller_key",   xxhash64(col("seller_email")))
    .withColumn("product_key",  xxhash64(col("product_name"), col("product_brand"),
                                          col("product_category"), col("product_color"),
                                          col("product_size")))
    .withColumn("store_key",    xxhash64(col("store_name"), col("store_city")))
    .withColumn("supplier_key", xxhash64(col("supplier_name"), col("supplier_city")))
    .withColumn("sale_dt",      to_date(col("sale_date"), "M/d/yyyy"))
    .withColumn("release_dt",   to_date(col("product_release_date"), "M/d/yyyy"))
    .withColumn("expiry_dt",    to_date(col("product_expiry_date"), "M/d/yyyy"))
    .withColumn("quantity",     col("sale_quantity").cast("int"))
    .withColumn("total_price",  col("sale_total_price").cast("decimal(18,2)"))
    .withColumn("unit_price",   col("product_price").cast("decimal(18,2)"))
    .withColumn("rating",       col("product_rating").cast("double"))
    .withColumn("reviews",      col("product_reviews").cast("int"))
    .withColumn("cust_age",     col("customer_age").cast("int"))
    .withColumn("prod_weight",  col("product_weight").cast("double"))
)

def write(df, table):
    (df.write.mode("overwrite")
       .option("truncate", "false")
       .jdbc(PG_URL, table, properties=PG_PROPS))
    print(f"PostgreSQL.{table}: {df.count()} строк")

dim_customer = (raw.dropDuplicates(["customer_key"]).select(
    col("customer_key").alias("customer_id"),
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("cust_age").alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country"),
    col("customer_postal_code").alias("postal_code"),
    col("customer_pet_type").alias("pet_type"),
    col("customer_pet_name").alias("pet_name"),
    col("customer_pet_breed").alias("pet_breed"),
))

dim_seller = (raw.dropDuplicates(["seller_key"]).select(
    col("seller_key").alias("seller_id"),
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code"),
))

dim_product = (raw.dropDuplicates(["product_key"]).select(
    col("product_key").alias("product_id"),
    col("product_name").alias("name"),
    col("product_category").alias("category"),
    col("unit_price").alias("price"),
    col("prod_weight").alias("weight"),
    col("product_color").alias("color"),
    col("product_size").alias("size"),
    col("product_brand").alias("brand"),
    col("product_material").alias("material"),
    col("product_description").alias("description"),
    col("rating").alias("rating"),
    col("reviews").alias("reviews"),
    col("release_dt").alias("release_date"),
    col("expiry_dt").alias("expiry_date"),
    col("pet_category").alias("pet_category"),
))

dim_store = (raw.dropDuplicates(["store_key"]).select(
    col("store_key").alias("store_id"),
    col("store_name").alias("name"),
    col("store_location").alias("location"),
    col("store_city").alias("city"),
    col("store_state").alias("state"),
    col("store_country").alias("country"),
    col("store_phone").alias("phone"),
    col("store_email").alias("email"),
))

dim_supplier = (raw.dropDuplicates(["supplier_key"]).select(
    col("supplier_key").alias("supplier_id"),
    col("supplier_name").alias("name"),
    col("supplier_contact").alias("contact"),
    col("supplier_email").alias("email"),
    col("supplier_phone").alias("phone"),
    col("supplier_address").alias("address"),
    col("supplier_city").alias("city"),
    col("supplier_country").alias("country"),
))

dim_date = (raw.select(col("sale_dt").alias("date"))
    .filter(col("date").isNotNull())
    .dropDuplicates()
    .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date"))))

fact_sales = (raw
    .withColumn("sale_id", monotonically_increasing_id())
    .select(
        col("sale_id"),
        col("customer_key").alias("customer_id"),
        col("seller_key").alias("seller_id"),
        col("product_key").alias("product_id"),
        col("store_key").alias("store_id"),
        col("supplier_key").alias("supplier_id"),
        date_format(col("sale_dt"), "yyyyMMdd").cast("int").alias("date_id"),
        col("sale_dt").alias("sale_date"),
        col("quantity"),
        col("total_price"),
    ))

write(dim_customer, "dim_customer")
write(dim_seller,   "dim_seller")
write(dim_product,  "dim_product")
write(dim_store,    "dim_store")
write(dim_supplier, "dim_supplier")
write(dim_date,     "dim_date")
write(fact_sales,   "fact_sales")

print("Модель 'звезда' в PostgreSQL построена")
spark.stop()
