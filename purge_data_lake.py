import os
import shutil
from datetime import datetime, timedelta

DATA_LAKE_PATH = "./data_lake"
DAYS_TO_KEEP = 7

def purge_old_partitions():
    print("🧹 Démarrage du nettoyage...")
    cutoff_date = datetime.now() - timedelta(days=DAYS_TO_KEEP)

    for feed in os.listdir(DATA_LAKE_PATH):
        feed_path = os.path.join(DATA_LAKE_PATH, feed)
        if not os.path.isdir(feed_path):
            continue

        for date_folder in os.listdir(feed_path):
            try:
                folder_date = datetime.strptime(date_folder, '%Y-%m-%d')
                if folder_date < cutoff_date:
                    full_path = os.path.join(feed_path, date_folder)
                    shutil.rmtree(full_path)
                    print(f"🗑️ Supprimé : {full_path}")
            except ValueError:
                continue

if __name__ == "__main__":
    purge_old_partitions()
