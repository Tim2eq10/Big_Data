from confluent_kafka import Consumer, Producer
import json
import pandas as pd
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

consumer_conf = {
    'bootstrap.servers': 'kafka-0:9097,kafka-1:9098',  
    'group.id': 'preproc_consumers',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': 'kafka-0:9097,kafka-1:9098'
}

def preprocess(data):
    """
    Простой препроцессинг данных:
    - Сохраняет столбец 'target' (если он существует)
    - Удаляет другие ненужные столбцы ('ID_code')
    - Возвращает обработанный DataFrame
    """
    try:
        if 'ID_code' in data.columns:
            data = data.drop(columns=['ID_code'], axis=1)  

        logging.info(f"Данные после препроцессинга: {data.head()}")
        return data

    except Exception as e:
        logging.error(f"Ошибка при препроцессинге данных: {e}")
        return None

def main():
    time.sleep(10)  
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['raw_data_1', 'raw_data_2'])  
    
    producer = Producer(producer_conf)
    logging.info("Начало работы скрипта dataproc.py")
    
    while True:
        msg = consumer.poll(1.0)  
        if msg is None:
            continue  
        if msg.error():
            logging.error(f"Ошибка при чтении сообщения: {msg.error()}")
            continue
        
        try:
            
            logging.info(f"Получено сообщение из топика: {msg.topic()}")
            
            json_msg = json.loads(msg.value().decode('utf-8'))
            df = pd.DataFrame([json_msg])  
            logging.info(f"Полученные данные: {df}")
            
            processed_df = preprocess(df)
            if processed_df is None:
                logging.warning("Препроцессинг не выполнен, пропускаем запись.")
                continue
            
            processed_json = processed_df.to_json(orient='records')
            logging.info(f"Отправленные данные: {processed_json}")
            
            producer.produce('processed_data', value=processed_json.encode('utf-8'))
            producer.flush()  
            logging.info("Обработанные данные успешно отправлены в топик 'processed_data'")
        except Exception as e:
            logging.error(f"Ошибка при обработке данных: {e}")

if __name__ == '__main__':
    main()