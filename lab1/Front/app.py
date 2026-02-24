import streamlit as st
import time
import json
from confluent_kafka import Consumer
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


consumer_conf = {
    'bootstrap.servers': 'kafka-0:9097,kafka-1:9098', 
    'group.id': 'streamlit_consumers',
    'auto.offset.reset': 'earliest'  
}


st.title("Визуализация предсказаний в реальном времени")


consumer = Consumer(consumer_conf)
consumer.subscribe(['predictions'])


prediction_counts = {"Class 0": 0, "Class 1": 0}
var_12_var_81_data = {'var_12': [], 'var_81': []}


true_labels = []
predictions_list = []


placeholder = st.empty()
while True:
    with placeholder.container():

        msg = consumer.poll(1.0)  
        if msg is not None and not msg.error():
            try:

                json_msg = json.loads(msg.value().decode('utf-8'))
                

                features = json_msg.get('features', {})
                prediction = json_msg.get('prediction', None)
                target = json_msg.get('target', None)
                

                if prediction is not None and prediction in [0, 1]:
                    prediction_counts[f"Class {prediction}"] += 1
                

                if target is not None and prediction is not None:
                    true_labels.append(int(target))
                    predictions_list.append(int(prediction))
                

                if 'var_12' in features and 'var_81' in features:
                    var_12_var_81_data['var_12'].append(float(features['var_12']))
                    var_12_var_81_data['var_81'].append(float(features['var_81']))

                MAX_DATA_POINTS = 100

                if len(true_labels) > MAX_DATA_POINTS:
                    true_labels = true_labels[-MAX_DATA_POINTS:]
                if len(predictions_list) > MAX_DATA_POINTS:
                    predictions_list = predictions_list[-MAX_DATA_POINTS:]
                if len(var_12_var_81_data['var_12']) > MAX_DATA_POINTS:
                    var_12_var_81_data['var_12'] = var_12_var_81_data['var_12'][-MAX_DATA_POINTS:]
                if len(var_12_var_81_data['var_81']) > MAX_DATA_POINTS:
                    var_12_var_81_data['var_81'] = var_12_var_81_data['var_81'][-MAX_DATA_POINTS:]



                col1, col2 = st.columns(2)


                with col1:
                    labels = list(prediction_counts.keys())
                    values = list(prediction_counts.values())
                    fig1, ax1 = plt.subplots(figsize=(6, 4))
                    ax1.bar(labels, values, color=['blue', 'green'])
                    ax1.set_title("Распределение классов")
                    ax1.set_ylabel("Количество")
                    st.pyplot(fig1)


                with col2:
                    if len(true_labels) > 0 and len(predictions_list) > 0:
                        accuracy = accuracy_score(true_labels, predictions_list)
                        precision = precision_score(true_labels, predictions_list, average='binary', zero_division=0)
                        recall = recall_score(true_labels, predictions_list, average='binary', zero_division=0)
                        f1 = f1_score(true_labels, predictions_list, average='binary', zero_division=0)
                        metrics = {
                            'Accuracy': accuracy,
                            'Precision': precision,
                            'Recall': recall,
                            'F1-Score': f1
                        }
                        fig2, ax2 = plt.subplots(figsize=(6, 4))
                        ax2.bar(metrics.keys(), metrics.values(), color=['orange', 'green', 'blue', 'purple'])
                        ax2.set_title("Метрики качества предсказания")
                        ax2.set_ylabel("Значение метрики")
                        st.pyplot(fig2)


                fig3, ax3 = plt.subplots(figsize=(12, 6))
                ax3.scatter(var_12_var_81_data['var_12'], var_12_var_81_data['var_81'], color='purple')
                ax3.set_title("Зависимость var_12 от var_81")
                ax3.set_xlabel("var_12")
                ax3.set_ylabel("var_81")
                st.pyplot(fig3)


                total_predictions = sum(values)
                st.write(f"Общее количество предсказаний: {total_predictions}")
                st.write(f"Счетчик классов: {prediction_counts}")

            except Exception as e:
                st.error(f"Ошибка при обработке данных: {e}")
        

        time.sleep(1)