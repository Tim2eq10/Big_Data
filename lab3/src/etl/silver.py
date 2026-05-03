# src/etl/silver.py
import polars as pl
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
import logging

def table_exists(path: str) -> bool:
    try:
        DeltaTable(path)
        return True
    except TableNotFoundError:  # <-- Исправленный импорт
        return False
    except Exception:
        return False

def clean_and_transform(bronze_path: str, silver_path: str):
    logging.info("Starting Silver transformation")
    
    df = pl.scan_delta(bronze_path)
    
    # Выбор колонок
    df = df.select([
        "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate",
        "Marketing_Airline_Network", "Origin", "Dest", "CRSDepTime", "DepTime",
        "DepDelay", "DepDelayMinutes", "ArrDelay", "ArrDelayMinutes",
        "Cancelled", "Diverted", "Distance"
    ]).rename({
        "Marketing_Airline_Network": "Airline",
        "DepDelay": "DepartureDelay",
        "ArrDelay": "ArrivalDelay"
    })
    
    # Фильтрация
    df = df.filter(
        (pl.col("Cancelled") == 0) & (pl.col("Diverted") == 0)
    )
    df = df.filter(
        pl.col("DepartureDelay").is_not_null() & pl.col("ArrivalDelay").is_not_null()
    )
    df = df.filter(
        (pl.col("DepartureDelay").abs() <= 360) & (pl.col("ArrivalDelay").abs() <= 360)
    )
    
    # Производные признаки
    df = df.with_columns([
        (pl.col("CRSDepTime") // 100).alias("hour"),
        pl.col("DayOfWeek").alias("day_of_week"),
        pl.col("Month").alias("month"),
        pl.when(pl.col("Month").is_between(3, 5)).then(pl.lit("spring"))
         .when(pl.col("Month").is_between(6, 8)).then(pl.lit("summer"))
         .when(pl.col("Month").is_between(9, 11)).then(pl.lit("fall"))
         .otherwise(pl.lit("winter")).alias("season"),
        (pl.col("Origin") + "-" + pl.col("Dest")).alias("route")
    ])
    
    df_final = df.select([
        "Year", "Month", "DayofMonth", "day_of_week", "FlightDate",
        "Airline", "Origin", "Dest", "route", "hour", "season",
        "DepartureDelay", "ArrivalDelay", "Distance"
    ])
    
    # Вывод плана
    logging.info("Optimized plan for Silver:\n" + df_final.explain(optimized=True))
    
    # Сбор данных
    df_collected = df_final.collect()
    logging.info(f"Silver rows after cleaning: {len(df_collected)}")
    
    # MERGE: если таблица существует, объединяем с новыми данными, убираем дубликаты
    if table_exists(silver_path):
        logging.info("Silver table already exists, merging new data")
        existing = pl.read_delta(silver_path)
        combined = pl.concat([existing, df_collected])
        key_cols = ["Year", "Month", "DayofMonth", "Airline", "FlightDate", "Origin", "Dest"]
        combined = combined.unique(subset=key_cols, keep="last")
        df_collected = combined
        logging.info(f"Merged data size: {len(df_collected)}")
    
    # Конвертация Polars -> Arrow и запись
    df_arrow = df_collected.to_arrow()
    write_deltalake(
        silver_path,
        df_arrow,
        mode="overwrite",
        partition_by=["Year", "Month"],
        engine="rust"
    )
    
    # Оптимизации Delta
    dt = DeltaTable(silver_path)
    logging.info("Running OPTIMIZE (compaction)...")
    dt.optimize.compact()
    logging.info("Running Z-ORDER on 'Origin' column...")
    dt.optimize.z_order(["Origin"])
    
    logging.info(f"Silver table saved to {silver_path}")
    return silver_path
