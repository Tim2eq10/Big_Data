import csv
import os
import json
import random
import time
from confluent_kafka import Producer

def create_kafka_producer():
    kafka_config = {
        'bootstrap.servers': 'kafka-0:9097,kafka-1:9098', 
        'client.id': 'csv-producer'
    }
    return Producer(kafka_config)

def send_message(producer, topic, message):
    try:
        producer.produce(topic, value=message.encode('utf-8'))
        producer.flush()
        print(f"Отправлено сообщение: {message}")
    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")

def read_random_line(file_path):
    with open(file_path, mode='r') as file:
        header = file.readline().strip()
        if not header:
            raise ValueError("Файл пуст или не содержит заголовка.")
        
        total_lines = sum(1 for line in file)
        if total_lines == 0:
            raise ValueError("Файл не содержит данных.")
        
        file.seek(0)
        next(file)  
        
        random_line_number = random.randint(1, total_lines)
        for _ in range(random_line_number):
            chosen_line = next(file).strip()
        
        row = dict(zip(header.split(','), chosen_line.split(',')))
        return row

def simulate_data_stream(file_path, topic, producer):
    while True:
        try:
            random_record = read_random_line(file_path)
            json_message = json.dumps(random_record)
            send_message(producer, topic, json_message)
            time.sleep(random.uniform(1, 2))  
        except Exception as e:
            print(f"Ошибка при чтении файла: {e}")
            break

if __name__ == "__main__":
    time.sleep(10)  
    
    
    producer1 = create_kafka_producer()
    producer2 = create_kafka_producer()
    
    
    from threading import Thread
    
    thread1 = Thread(target=simulate_data_stream, args=('/app/data1.csv', 'raw_data_1', producer1))
    thread2 = Thread(target=simulate_data_stream, args=('/app/data2.csv', 'raw_data_2', producer2))
    
    thread1.start()
    thread2.start()
    
    thread1.join()
    thread2.join()