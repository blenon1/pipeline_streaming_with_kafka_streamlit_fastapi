import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import json
import os
import pandas as pd

TOPIC = 'transaction_log'
BOOTSTRAP_SERVER = 'localhost:9092'
DATA_LAKE_BASE = './data_lake'

class ParseKafkaMessage(beam.DoFn):
    def process(self, element):
        _, value = element
        tx = json.loads(value.decode('utf-8'))
        tx['__received_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        yield tx

class WriteParquetByDate(beam.DoFn):
    def process(self, elements):
        if not elements:
            return
        df = pd.DataFrame(elements)
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_dir = os.path.join(DATA_LAKE_BASE, TOPIC, date_str)
        os.makedirs(output_dir, exist_ok=True)
        file_name = f"beam_{datetime.now().strftime('%H%M%S')}.parquet"
        df.to_parquet(os.path.join(output_dir, file_name), index=False)
        print(f"✅ {len(df)} lignes écrites dans {output_dir}")
        return

def run():
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Lire Kafka' >> beam.io.ReadFromKafka(
                consumer_config={'bootstrap.servers': BOOTSTRAP_SERVER},
                topics=[TOPIC])
            | 'Parser JSON' >> beam.ParDo(ParseKafkaMessage())
            | 'Batcher' >> beam.BatchElements(min_batch_size=5, max_batch_size=50)
            | 'Écrire Parquet' >> beam.ParDo(WriteParquetByDate())
        )

if __name__ == "__main__":
    run()
