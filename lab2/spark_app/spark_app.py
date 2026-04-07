import argparse
import os
import time
import json
import threading
import psutil

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    BooleanType, DoubleType
)

RESULT_JSON_PATH = '/opt/spark-app/results.json'

schema = StructType([
    StructField("", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("target", IntegerType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("adult", BooleanType(), True),
    StructField("budget", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("tagline", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("production_companies", StringType(), True),
    StructField("production_countries", StringType(), True),
    StructField("spoken_languages", StringType(), True),
    StructField("keywords", StringType(), True)
])

def memory_monitor(monitor_list, stop_event, start_time, interval=0.5):
    process = psutil.Process(os.getpid())
    while not stop_event.is_set():
        elapsed = time.time() - start_time
        mem_usage_mb = process.memory_info().rss / (1024 * 1024)
        monitor_list.append((elapsed, mem_usage_mb))
        time.sleep(interval)

def main(optimize, experiment_key):
    try:
        spark = SparkSession.builder \
            .appName("HadoopSparkExperiment") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        print("Spark session создан. Оптимизации включены:", optimize)

        start_time = time.time()
        memory_usage_series = []
        stop_event = threading.Event()
        monitor_thread = threading.Thread(target=memory_monitor, args=(memory_usage_series, stop_event, start_time))
        monitor_thread.start()

        print("Чтение данных из HDFS...")
        df = (spark.read.option("header", "true")
              .schema(schema)
              .csv("hdfs://namenode:9000/user/hadoop/test_data.csv"))

        df.printSchema()
        df.show(10)

        count_initial = df.count()
        print(f"Количество строк в датасете: {count_initial}")

        if optimize:
            print("Применяем оптимизации: repartition и cache")
            df = df.repartition(4)
            df.cache()
            df.count()

        t_opt_start = time.time()  # Время для операций (без оверхэда)

        print("Выполняется агрегация по полю original_language...")
        agg_df = df.groupBy("original_language").count()
        results = agg_df.collect()
        print("Результаты агрегации:")
        print(results[:5])

        print("Сортировка агрегированных результатов по убыванию count...")
        sorted_df = agg_df.orderBy("count", ascending=False)
        sorted_results = sorted_df.collect()
        print(sorted_results[:5])

        t1 = time.time()
        elapsed_operations = t1 - t_opt_start
        elapsed_total = t1 - start_time
        print(f"Общее время выполнения: {elapsed_total:.2f} секунд")
        print(f"Время выполнения операций после оптимизации: {elapsed_operations:.2f} секунд")

        stop_event.set()
        monitor_thread.join()

        final_mem_usage = memory_usage_series[-1][1] if memory_usage_series else 0.0
        print("Финальное использование памяти: {:.2f} MB".format(final_mem_usage))

        results_data = {
            "ops_time": elapsed_operations,
            "total_time": elapsed_total,
            "final_memory_usage": final_mem_usage,
            "memory_usage_over_time": memory_usage_series
        }

        if os.path.exists(RESULT_JSON_PATH):
            with open(RESULT_JSON_PATH, 'r', encoding="utf-8") as json_file:
                all_results = json.load(json_file)
        else:
            all_results = {}

        all_results[experiment_key] = results_data

        with open(RESULT_JSON_PATH, 'w') as json_file:
            json.dump(all_results, json_file, indent=4)
        print(f"Результаты эксперимента '{experiment_key}' сохранены в {RESULT_JSON_PATH}")

        spark.stop()
    except Exception as e:
        print("Произошла ошибка при выполнении Spark-приложения:")
        print(e)
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Application для экспериментов с Hadoop"
    )
    parser.add_argument("--optimize", action="store_true",
                        help="Применять оптимизации Spark (cache, repartition)")
    parser.add_argument("--experiment-key", type=str, required=True,
                        help="Ключ эксперимента вида '<datanode>_<plain|opt>'")
    args = parser.parse_args()

    main(args.optimize, args.experiment_key)
