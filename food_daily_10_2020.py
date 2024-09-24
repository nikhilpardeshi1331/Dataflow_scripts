import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class DataIngestion:
    def parse_method(self, string_input):
        # Parsing the input string and creating a dictionary for each row
        values = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', re.sub('\r\n', '', string_input))
        row = dict(zip(('Customer_id', 'date', 'time', 'order_id', 'items', 'amount', 'mode', 'restaurant', 'status', 'ratings', 'feedback'), values))
        return row

def run():
    # Setting up the Apache Beam pipeline options.
    beam_options = PipelineOptions(
        runner='DataflowRunner',
        project='lunar-byte-425616-p6',
        temp_location='gs://lunar-byte-425616-p6/temp',
        region='us-central1'
    )
    
    # Pipeline creation
    p = beam.Pipeline(options=beam_options)

    # Define your Google Cloud Storage CSV file path.
    input_file = 'gs://lunar-byte-425616-p6/data_files/food_daily_10_2020.csv'
    
    # Define BigQuery output table name.
    output_table = 'lake.orders_table'

    # Pipeline steps
    data_ingestion = DataIngestion()
    (p
     | 'Read from a File' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         output_table,
         schema=('Customer_id:STRING, date:STRING, time:STRING, order_id:STRING, items:STRING, '
                 'amount:INTEGER, mode:STRING, restaurant:STRING, status:STRING, ratings:INTEGER, '
                 'feedback:STRING'),
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
    # Pipeline execution
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
