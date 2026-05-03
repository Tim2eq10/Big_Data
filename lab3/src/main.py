# src/main.py
import logging
import os
import sys
from src.etl.bronze import load_bronze
from src.etl.silver import clean_and_transform
from src.etl.gold import create_gold
from src.ml.model import train_models

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/app.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    logger = logging.getLogger(__name__)
    logger.info("Starting Flight Lakehouse pipeline with Polars + Delta")
    
    # Пути (локальные внутри контейнера)
    csv_path = "/app/data/flight_data_2018_2024.csv"   # путь к вашему CSV файлу
    bronze_path = "/app/data/bronze"
    silver_path = "/app/data/silver"
    gold_agg_path = "/app/data/gold_agg"
    gold_feat_path = "/app/data/gold_feat"
    
    # Убедимся, что директории существуют
    for p in [bronze_path, silver_path, gold_agg_path, gold_feat_path]:
        os.makedirs(p, exist_ok=True)
    
    try:
        # Bronze
        load_bronze(csv_path, bronze_path)
        
        # Silver (с MERGE и partition_by)
        clean_and_transform(bronze_path, silver_path)
        
        # Gold
        create_gold(silver_path, gold_agg_path, gold_feat_path)
        
        # ML
        train_models(gold_feat_path)
        
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
