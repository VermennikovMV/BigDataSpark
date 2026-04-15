#!/usr/bin/env bash
# Запуск всех трёх Spark-джобов последовательно.
# Требует поднятых контейнеров: docker compose up -d
set -euo pipefail

# Выключаем MSYS-преобразование Unix-путей в Windows-пути (Git Bash),
# иначе /opt/spark-apps превращается в C:/Program Files/Git/opt/spark-apps.
export MSYS_NO_PATHCONV=1

PACKAGES="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.3.1"

run_job() {
    local job="$1"
    echo "==> spark-submit $job"
    docker compose exec -T spark /opt/spark/bin/spark-submit \
        --master "local[*]" \
        --packages "$PACKAGES" \
        --conf spark.jars.ivy=/root/.ivy2 \
        "/opt/spark-apps/$job"
}

run_job 00_load_raw.py
run_job 01_star_schema.py
run_job 02_clickhouse_reports.py

echo "Пайплайн завершён."
