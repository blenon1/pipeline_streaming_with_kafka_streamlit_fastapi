import pandas as pd
import json
import os
from datetime import datetime
from kafka import KafkaConsumer

# Load the data
path_data = './messages_transaction_log_43.csv'
df = pd.read_csv(path_data)

# Display information about the DataFrame
print(df.info())
print(df.head())
print(df.describe())

# Convert the 'value' column from DataFrame to a JSON string
df['parsed_value'] = df['value'].apply(json.loads)

#  Normalize the JSON values to flatten them into columns
df_parsed = pd.json_normalize(df['parsed_value'])

# Display  dataframe to user
# tools.display_dataframe_to_user(name="Transactions Parsed", dataframe=df_parsed)
print("Displaying DataFrame to user:")
print(df_parsed)

# Script Python Kafka Consumer vers Data Lake (partition par date)
TOPIC = "transaction_log"
DATA_LAKE_PATH = "./data_lake"
BOOTSTRAP_SERVERS = ["localhost:9092"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def write_to_lake(data):
    # Récupérer la date d'ingestion
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    partition_path = os.path.join(DATA_LAKE_PATH, TOPIC, date_str)
    os.makedirs(partition_path, exist_ok=True)

    df = pd.DataFrame(data)

    # Sauvegarder en parquet
    file_name = f"{datetime.now().strftime('%H%M%S')}_batch.parquet"
    file_path = os.path.join(partition_path, file_name)
    df.to_parquet(file_path, index=False)
    print(f">>> {len(df)} lignes sauvegardées dans {file_path}")

if __name__ == "__main__":
    print(f"👂 En écoute sur Kafka topic: {TOPIC}")
    buffer = []
    BATCH_SIZE = 20

    try:
        for message in consumer:
            parsed = message.value
            parsed["__received_at"] = datetime.now().isoformat()
            buffer.append(parsed)

            if len(buffer) >= BATCH_SIZE:
                write_to_lake(buffer)
                buffer.clear()

    except KeyboardInterrupt:
        print("❌ Arrêt du consumer.")
        if buffer:
            write_to_lake(buffer)

