import json
from kafka import KafkaConsumer
import mysql.connector
from datetime import datetime

TOPIC = "transaction_log"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Connexion MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="R@kuzan19735.",  # Remplace ici
    database="data_warehouse"
)
cursor = db.cursor()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def insert_user(user_id, user_name):
    cursor.execute("INSERT IGNORE INTO users (user_id, user_name) VALUES (%s, %s)", (user_id, user_name))

def insert_transaction(tx):
    timestamp_clean = None  # Initialisation de timestamp_clean à None
    # Conversion du timestamp ISO 8601 vers format MySQL
    try:
        timestamp_raw = tx["timestamp"]
        # Si le timestamp finit par 'Z', on le remplace par '+00:00' pour indiquer explicitement UTC
        if timestamp_raw.endswith("Z"):
            timestamp_raw = timestamp_raw[:-1] + "+00:00"
        parsed_timestamp = datetime.fromisoformat(timestamp_raw)
        timestamp_clean = parsed_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"***DEBUG*** Timestamp après conversion: {timestamp_clean}") # Ajout de cette ligne pour le débogage
    except ValueError as e:
        print(f"⛔ Erreur de format date : {tx['timestamp']} -> {e}")
        return  # on skippe cette ligne

    if timestamp_clean is not None:  # Vérifiez si timestamp_clean a été assigné
        cursor.execute("""
            INSERT INTO transactions (
                transaction_id, timestamp, user_id, product_id, amount, currency,
                transaction_type, status, payment_method,
                location_city, location_country, received_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE status=VALUES(status)
        """, (
            tx["transaction_id"],
            timestamp_clean,
            tx["user_id"],
            tx["product_id"],
            tx["amount"],
            tx["currency"],
            tx["transaction_type"],
            tx["status"],
            tx["payment_method"],
            tx.get("location", {}).get("city"),
            tx.get("location", {}).get("country"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
def insert_shipping(tx):
    cursor.execute("""
        INSERT INTO shipping_addresses (
            transaction_id, street, zip_code, city, country
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE zip_code=VALUES(zip_code)
    """, (
        tx["transaction_id"],
        tx.get("shipping_address", {}).get("street"),
        tx.get("shipping_address", {}).get("zip"),
        tx.get("shipping_address", {}).get("city"),
        tx.get("shipping_address", {}).get("country")
    ))

def insert_device(tx):
    cursor.execute("""
        INSERT INTO device_info (
            transaction_id, os, browser, ip_address
        ) VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE browser=VALUES(browser)
    """, (
        tx["transaction_id"],
        tx.get("device_info", {}).get("os"),
        tx.get("device_info", {}).get("browser"),
        tx.get("device_info", {}).get("ip_address")
    ))

print("📡 En écoute sur Kafka...")

try:
    for msg in consumer:
        tx = msg.value
        insert_user(tx["user_id"], tx["user_name"])
        insert_transaction(tx)
        insert_shipping(tx)
        insert_device(tx)
        db.commit()
        print(f"✅ Transaction {tx['transaction_id']} insérée")
except KeyboardInterrupt:
    print("❌ Arrêt du consumer.")
finally:
    cursor.close()
    db.close()
