# -*- coding: utf-8 -*-
"""
Productor de Kafka - Anexo 3 de la guía (Tarea 3, Big Data UNAD).
Genera cada segundo una lectura simulada de un sensor y la publica en el
topic 'sensor_data' del broker local (localhost:9092).

Requisitos: pip install kafka-python   |   Ejecución: python3 kafka_producer.py
Detener con Ctrl+C.
"""
import time
import json
import random
from kafka import KafkaProducer


def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),                 # 10 sensores simulados
        "temperature": round(random.uniform(20, 30), 2),    # grados Celsius
        "humidity": round(random.uniform(30, 70), 2),       # porcentaje
        "timestamp": int(time.time())                       # segundos desde 1970 (época Unix)
    }


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    sensor_data = generate_sensor_data()
    producer.send('sensor_data', value=sensor_data)
    print(f"Sent: {sensor_data}")
    time.sleep(1)
