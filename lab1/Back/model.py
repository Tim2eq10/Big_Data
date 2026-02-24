from confluent_kafka import Consumer, Producer
import json
import joblib
import pandas as pd
import logging
import time


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

consumer_conf = {
    'bootstrap.servers': 'kafka-0:9097,kafka-1:9098',
    'group.id': 'model_consumers',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': 'kafka-0:9097,kafka-1:9098'
}


model = joblib.load('/app/trained_model.joblib')

def main():
    time.sleep(10)  

    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['processed_data'])

    
    producer = Producer(producer_conf)

    logging.info("Начало работы скрипта model.py")

    while True:
        
        msg = consumer.poll(1.0)  
        if msg is None:
            continue  

        if msg.error():
            logging.error(f"Ошибка при чтении сообщения: {msg.error()}")
            continue

        try:
            
            json_msg = msg.value().decode('utf-8')
            logging.info(f"Полученное сообщение: {json_msg}")

            
            data = json.loads(json_msg)
            df = pd.DataFrame(data)

            
            target = df['target'].values[0] if 'target' in df.columns else None

            
            if 'target' in df.columns:
                df = df.drop(columns=['target'], axis=1)

            logging.info(f"Данные после удаления 'target': {df}")

            
            predictions = model.predict(df)

            
            result = {
                'features': data[0],  
                'prediction': int(predictions[0]),  
                'target': target  
            }

            logging.info(f"Результат: {result}")

            
            producer.produce('predictions', value=json.dumps(result).encode('utf-8'))
            producer.flush()  
            logging.info("Результаты предсказаний успешно отправлены в топик 'predictions'")

        except Exception as e:
            logging.error(f"Ошибка при обработке данных: {e}")

if __name__ == '__main__':
    main()