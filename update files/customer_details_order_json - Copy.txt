import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

# Setting up the Apache Beam pipeline options.
beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='lunar-byte-425616-p6',
    temp_location='gs://bq_project_nik/temp',
    region='us-west1'
)

class DataIngestion:
    def parse_method(self, string_input):
        # Parsing the input string and creating a dictionary for each row
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '', string_input)))
        row = dict(zip(('Customer_id', 'date', 'time', 'order_id', 'items', 'amount', 'mode', 'restaurant', 'status', 'ratings', 'feedback'), values))
        return row

def run():

    # Define your Google Cloud Storage CSV file path.
    input_file = 'gs://lunar-byte-425616-p6/data_files/customer_details.csv'

    # Define BigQuery output table name.
    output_table = 'lake.orders_table_json_schema'

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
     | 'Read from a File' >> beam.io.ReadFromText(input_file)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
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
