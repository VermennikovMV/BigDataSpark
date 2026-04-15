"""
Загрузка исходных CSV-файлов (10 файлов MOCK_DATA*.csv по 1000 строк)
в таблицу mock_data в PostgreSQL. Заменяет ручной импорт через DBeaver
из шага 6 лабораторной.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

spark = (SparkSession.builder
         .appName("load_raw")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("escape", '"')
      .option("quote", '"')
      .csv("/data/csv/MOCK_DATA*.csv"))

df = df.select(*[col(c).cast("string").alias(c) for c in df.columns])

print(f"Прочитано строк из CSV: {df.count()}")

(df.write
   .mode("overwrite")
   .option("truncate", "false")
   .jdbc(PG_URL, "mock_data", properties=PG_PROPS))

print("mock_data успешно загружена в PostgreSQL")
spark.stop()
