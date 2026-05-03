import polars as pl
from deltalake import write_deltalake
import logging

def load_bronze(csv_path: str, bronze_path: str):
    logging.info(f"Loading CSV from {csv_path} into Bronze with daily batches")
    
    df_all = pl.read_csv(csv_path, try_parse_dates=False)
    
    # Проверяем, какие колонки есть для определения даты
    if "FlightDate" in df_all.columns:
        logging.info("Using FlightDate column for daily grouping")
        # Приводим к строковому формату YYYY-MM-DD (если нужно)
        df_all = df_all.with_columns(
            pl.col("FlightDate").cast(pl.Utf8).alias("Date")
        )
    elif all(col in df_all.columns for col in ["Year", "Month", "DayofMonth"]):
        logging.info("Creating Date from Year, Month, DayofMonth")
        df_all = df_all.with_columns(
            (pl.col("Year").cast(pl.Utf8) + "-" +
             pl.col("Month").cast(pl.Utf8).str.zfill(2) + "-" +
             pl.col("DayofMonth").cast(pl.Utf8).str.zfill(2)).alias("Date")
        )
    else:
        raise ValueError("CSV must contain FlightDate or (Year, Month, DayofMonth) columns")
    
    # Получаем список уникальных дат, отсортированных хронологически
    dates = df_all.select("Date").unique().sort("Date").to_series().to_list()
    logging.info(f"Found {len(dates)} daily batches (first 5: {dates[:5]})")
    
    first = True
    for date in dates:
        df_day = df_all.filter(pl.col("Date") == date).drop("Date")
        logging.info(f"Processing batch {date}, rows: {len(df_day)}")
        
        mode = "overwrite" if first else "append"
        df_arrow = df_day.to_arrow()
        write_deltalake(bronze_path, df_arrow, mode=mode, engine="rust")
        first = False
    
    logging.info(f"Bronze table created at {bronze_path} with daily version history")
    return bronze_path
