import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.avroio import ReadFromAvro

# Setting up the Apache Beam pipeline options.
beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='lunar-byte-425616-p6',
    temp_location='gs://bq_project_nik/temp',
    region='us-west1'
)

class DataIngestion:
    def parse_method(self, avro_record):
        # Directly use the Avro record as the row dictionary
        row = {
            'Customer_id': avro_record['CustomerID'],       # Make sure these keys match your Avro schema
            'date': avro_record['OrderDate'],               # Make sure these keys match your Avro schema
            'time': avro_record['OrderTimestamp'],          # Make sure these keys match your Avro schema
            'order_id': avro_record['OrderID'],             # Make sure these keys match your Avro schema
            'items': avro_record['Item'],                   # Make sure these keys match your Avro schema
            'amount': avro_record['Quantity'],              # Make sure these keys match your Avro schema
            'mode': avro_record['PaymentMethod'],           # Make sure these keys match your Avro schema
            'restaurant': avro_record['Restaurant'],        # Make sure these keys match your Avro schema
            'status': avro_record['Status'],                # Make sure these keys match your Avro schema
            'ratings': avro_record['Rating'],               # Make sure these keys match your Avro schema
            'feedback': avro_record['Feedback']             # Make sure these keys match your Avro schema
        }
        return row

def run():

    # Define your Google Cloud Storage Avro file path.
    input_file = 'gs://lunar-byte-425616-p6/data_files/unique_customer_details.avro'

    # Define BigQuery output table name.
    output_table = 'lake.unique_customer_details_avro_2'

    # Define schema with date and timestamp fields
    schema_str = """
    {
      "fields": [
        {"name": "Customer_id", "type": "STRING"},
        {"name": "date", "type": "DATE"},
        {"name": "time", "type": "TIMESTAMP"},
        {"name": "order_id", "type": "STRING"},
        {"name": "items", "type": "STRING"},
        {"name": "amount", "type": "INTEGER"},
        {"name": "mode", "type": "STRING"},
        {"name": "restaurant", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "ratings", "type": "INTEGER"},
        {"name": "feedback", "type": "STRING"}
      ]
    }
    """

    table_schema = parse_table_schema_from_json(schema_str)

    # Pipeline creation
    p = beam.Pipeline(options=beam_options)

    # Pipeline steps
    data_ingestion = DataIngestion()
    (p
     | 'Read from Avro File' >> ReadFromAvro(input_file)
     | 'Avro Record To BigQuery Row' >> beam.Map(lambda record: data_ingestion.parse_method(record))
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         output_table,
         schema=table_schema,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
    # Pipeline execution
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
