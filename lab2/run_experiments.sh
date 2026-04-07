#!/bin/bash
set -e

upload_dataset() {
  echo "Создаём директорию /user/hadoop в HDFS..."
#  docker exec namenode hdfs dfs -mkdir -p /user/hadoop || true
  docker exec namenode hdfs dfs -mkdir -p /user/hadoop

  echo "Загружаем test_data.csv в HDFS..."
#  docker exec namenode hdfs dfs -put -f /hadoop/test_data.csv /user/hadoop/test_data.csv || true
  docker exec namenode hdfs dfs -put -f /hadoop/test_data.csv /user/hadoop/test_data.csv
}

check_safemode() {
  echo "Проверяем состояние Safe Mode..."
  while true; do
    mode=$(docker exec namenode hdfs dfsadmin -safemode get)
    if echo "$mode" | grep -q "Safe mode is OFF"; then
      echo "Safe Mode отключён."
      break
    else
      echo "Safe Mode активен, ожидаем 5 секунд..."
      sleep 5
    fi
  done
}

wait_for_hdfs() {
  echo "Проверка доступности HDFS..."
  while true; do
    docker exec namenode hdfs dfs -ls /user/hadoop/test_data.csv &>/dev/null
    if [ $? -eq 0 ]; then
      echo "HDFS доступен."
      break
    else
      echo "HDFS ещё не доступен, повторяем попытку через 5 секунд..."
      sleep 5
    fi
  done
}

run_spark_job() {
  local mode="$1"        # plain or opt
  local nodes="$2"       # DataNode count (e.g. 1 or 3)
  local experiment_key="${nodes}_${mode}"
  echo "Запускаем Spark приложение. Режим: $experiment_key"

  if [ "$mode" == "opt" ]; then
    docker exec spark /spark/bin/spark-submit --master spark://spark:7077 \
      /opt/spark-app/spark_app.py --optimize --experiment-key "$experiment_key"
  else
    docker exec spark /spark/bin/spark-submit --master spark://spark:7077 \
      /opt/spark-app/spark_app.py --experiment-key "$experiment_key"
  fi
}

echo "Пересобираем контейнеры..."
docker-compose build --no-cache

# --- Эксперименты с 1 DataNode ---
echo "==================================================="
echo "Запуск кластера с 1 DataNode..."
docker-compose up -d --scale datanode=1

echo "Ожидание старта кластера (10 секунд)..."
sleep 10

check_safemode
upload_dataset
wait_for_hdfs

echo "---------------------------------------------------"
echo "Эксперимент 1: 1 DataNode, Spark Unoptimized"
run_spark_job "plain" 1

echo "---------------------------------------------------"
echo "Эксперимент 2: 1 DataNode, Spark Optimized"
run_spark_job "opt" 1

docker cp spark:/opt/spark-app/results.json ./results_1.json

echo "Остановка кластера с 1 DataNode..."
docker-compose down

# --- Эксперименты с 3 DataNode ---
echo "==================================================="
echo "Запуск кластера с 3 DataNodes..."
docker-compose up -d --scale datanode=3

echo "Ожидание старта кластера (10 секунд)..."
sleep 10

check_safemode
upload_dataset
wait_for_hdfs

echo "---------------------------------------------------"
echo "Эксперимент 3: 3 DataNodes, Spark Unoptimized"
run_spark_job "plain" 3

echo "---------------------------------------------------"
echo "Эксперимент 4: 3 DataNodes, Spark Optimized"
run_spark_job "opt" 3

docker cp spark:/opt/spark-app/results.json ./results_3.json
echo "Файл с результатами откопирован."

echo "Остановка кластера с 3 DataNodes..."
docker-compose down

echo "==================================================="
echo "Все эксперименты завершены!"

