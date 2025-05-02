import time
import schedule
import apache_beam as beam
from datetime import datetime

def beam_task():
    print(f"🚀 Lancement du job Beam à {datetime.now().strftime('%H:%M:%S')}")

    with beam.Pipeline() as p:
        (
            p
            | 'Start' >> beam.Create(["hello"])
            | 'Log' >> beam.Map(lambda x: print(f"Job Beam exécuté à {datetime.now()}"))
        )

# Planification toutes les 10 minutes
schedule.every(10).minutes.do(beam_task)

if __name__ == "__main__":
    print("⏳ L'orchestrateur Beam tourne toutes les 10 minutes...")
    beam_task()  # première exécution immédiate

    while True:
        schedule.run_pending()
        time.sleep(1)
