---

## Реализация (обязательный минимум: PostgreSQL + Spark + ClickHouse)

### Структура репозитория

```
.
├── docker-compose.yml              # PostgreSQL + ClickHouse + Spark
├── clickhouse-init/
│   └── 01_ddl.sql                  # DDL для 6 витрин в ClickHouse (БД reports)
├── spark-apps/
│   ├── 00_load_raw.py              # CSV (10 файлов) -> PostgreSQL.mock_data
│   ├── 01_star_schema.py           # mock_data -> модель "звезда" в PostgreSQL
│   └── 02_clickhouse_reports.py    # звезда -> 6 витрин в ClickHouse
├── run-pipeline.sh                 # запуск всех трёх джобов
└── исходные данные/                # MOCK_DATA*.csv (10 файлов × 1000 строк)
```

### Модель "звезда" в PostgreSQL (БД `bigdata`)

- **Факт:** `fact_sales` (sale_id, customer_id, seller_id, product_id, store_id, supplier_id, date_id, sale_date, quantity, total_price)
- **Измерения:** `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`, `dim_date`
- Суррогатные ключи измерений — `xxhash64` от естественных атрибутов (email клиента/продавца, связка имя+бренд+категория+цвет+размер для продукта, имя+город для магазина/поставщика). Это нужно, потому что `id`/`sale_*_id` в 10 CSV-файлах пересекаются.

### Витрины в ClickHouse (БД `reports`)

| Таблица | Что покрывает |
|---------|---------------|
| `product_sales_mart`   | выручка/количество/рейтинг/отзывы по продуктам (п.1) |
| `customer_sales_mart`  | сумма покупок, страна, средний чек по клиентам (п.2) |
| `time_sales_mart`      | выручка/заказы/средний размер заказа по годам-месяцам (п.3) |
| `store_sales_mart`     | выручка/средний чек по магазинам с городом и страной (п.4) |
| `supplier_sales_mart`  | выручка, средняя цена товара, страна поставщика (п.5) |
| `quality_mart`         | рейтинг, отзывы, продажи и выручка по продуктам (п.6) |

Конкретные отчёты (топ-10, распределение по странам и т.д.) получаются SQL-запросами к этим витринам: `SELECT ... ORDER BY total_revenue DESC LIMIT 10` и т.п.

### Как запустить

Требования: Docker Desktop.

```bash
# 1. Поднять инфраструктуру
docker compose up -d

# 2. Запустить все 3 Spark-джоба последовательно (Git Bash / WSL)
bash run-pipeline.sh

# Альтернатива — запускать по отдельности (Git Bash на Windows — обязательно MSYS_NO_PATHCONV=1):
export MSYS_NO_PATHCONV=1
PKGS="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.3.1"
docker compose exec -T spark /opt/spark/bin/spark-submit --master local[*] --packages $PKGS /opt/spark-apps/00_load_raw.py
docker compose exec -T spark /opt/spark/bin/spark-submit --master local[*] --packages $PKGS /opt/spark-apps/01_star_schema.py
docker compose exec -T spark /opt/spark/bin/spark-submit --master local[*] --packages $PKGS /opt/spark-apps/02_clickhouse_reports.py
```

### Как проверить результат

**PostgreSQL** (DBeaver / psql, localhost:5432, spark/spark, БД `bigdata`):
```sql
SELECT COUNT(*) FROM mock_data;     -- 10000
SELECT COUNT(*) FROM fact_sales;    -- 10000
SELECT COUNT(*) FROM dim_product;
```

**ClickHouse** (DBeaver / clickhouse-client, localhost:8123 HTTP или 9000 TCP, spark/spark, БД `reports`):
```sql
-- Топ-10 продуктов по выручке
SELECT product_name, total_revenue FROM reports.product_sales_mart ORDER BY total_revenue DESC LIMIT 10;

-- Топ-10 клиентов
SELECT customer_name, total_spent FROM reports.customer_sales_mart ORDER BY total_spent DESC LIMIT 10;

-- Месячные тренды
SELECT year, month, total_revenue FROM reports.time_sales_mart ORDER BY year, month;

-- Топ-5 магазинов
SELECT store_name, total_revenue FROM reports.store_sales_mart ORDER BY total_revenue DESC LIMIT 5;

-- Топ-5 поставщиков
SELECT supplier_name, total_revenue FROM reports.supplier_sales_mart ORDER BY total_revenue DESC LIMIT 5;

-- Продукты с лучшим/худшим рейтингом
SELECT product_name, rating FROM reports.quality_mart ORDER BY rating DESC LIMIT 10;
SELECT product_name, review_count FROM reports.quality_mart ORDER BY review_count DESC LIMIT 10;
```

### Остановка

```bash
docker compose down          # остановить, оставить данные
docker compose down -v       # полная очистка (вместе с томами)
```

