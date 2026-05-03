import polars as pl
from deltalake import write_deltalake, DeltaTable
import logging

def create_gold(silver_path: str, gold_agg_path: str, gold_feat_path: str):
    logging.info("Creating Gold layers from Silver")
    
    df_silver = pl.scan_delta(silver_path)
    
    # 1. Аналитическая витрина
    agg_df = (df_silver
              .group_by(["Origin", "Airline", "hour", "season"])
              .agg([
                  pl.mean("ArrivalDelay").alias("avg_arrival_delay"),
                  pl.mean("DepartureDelay").alias("avg_departure_delay"),
                  pl.count().alias("flight_count")
              ])
              .collect()
    )
    logging.info(f"Analytical gold rows: {len(agg_df)}")
    agg_arrow = agg_df.to_arrow()
    write_deltalake(gold_agg_path, agg_arrow, mode="overwrite", engine="rust")
    
    # 2. Feature table (для ML)
    feat_df = df_silver.select([
        "DepartureDelay", "ArrivalDelay", "hour", "day_of_week",
        pl.col("Month").alias("month"),  # <-- исправлено: переименовываем Month -> month
        "season", "Distance", "Airline", "Origin", "Dest"
    ]).collect()
    logging.info(f"Feature table rows: {len(feat_df)}")
    
    feat_arrow = feat_df.to_arrow()
    write_deltalake(gold_feat_path, feat_arrow, mode="overwrite", engine="rust")
    
    # Оптимизации
    dt_agg = DeltaTable(gold_agg_path)
    dt_agg.vacuum(retention_hours=168, dry_run=True)
    logging.info("VACUUM called on gold_agg table (dry-run)")
    
    dt_feat = DeltaTable(gold_feat_path)
    dt_feat.optimize.z_order(["Airline"])
    logging.info("Z-ORDER on feature table by Airline")
    
    return gold_agg_path, gold_feat_path