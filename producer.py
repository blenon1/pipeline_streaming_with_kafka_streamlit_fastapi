import json
import pandas as pd
from kafka import KafkaProducer
import time

# Configuration du broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # à adapter si besoin
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Charger le CSV
df = pd.read_csv("messages_transaction_log_43.csv")

# Nettoyer les champs "value" (en JSON)
json_records = df['value'].dropna().apply(json.loads)

# Envoyer les messages dans Kafka
TOPIC = "transaction_log"

for i, record in enumerate(json_records):
    producer.send(TOPIC, value=record)
    print(f"✅ Message {i+1} envoyé : {record.get('transaction_id', 'ID inconnu')}")
    time.sleep(0.1)  # petit délai pour simuler un flux progressif

producer.flush()
print("🎉 Tous les messages ont été envoyés.")
