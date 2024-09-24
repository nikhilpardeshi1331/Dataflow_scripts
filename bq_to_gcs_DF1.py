import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    # Setting up the Apache Beam pipeline options.
    beam_options = PipelineOptions(
        runner='DataflowRunner',
        project='lunar-byte-425616-p6',
        temp_location='gs://lunar-byte-425616-p6/temp',
        region='us-central1'
    )

    # Define BigQuery input table.
    input_table = 'lake.orders_table'

    # Define the output file path.
    output_file = 'gs://lunar-byte-425616-p6/output/orders_output.csv'

    # Define a function to format the output as CSV.
    def format_csv(row):
        return ','.join(str(row.get(col, '')) for col in [
            'Customer_id', 'date', 'time', 'order_id', 'items', 'amount', 'mode',
            'restaurant', 'status', 'ratings', 'feedback'
        ])

    # Pipeline creation
    p = beam.Pipeline(options=beam_options)

    # Pipeline steps
    (p
     | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(table=input_table)
     | 'Format as CSV' >> beam.Map(format_csv)
     | 'Write to GCS' >> beam.io.WriteToText(output_file, file_name_suffix='.csv', header='Customer_id,date,time,order_id,items,amount,mode,restaurant,status,ratings,feedback')
    )
    
    # Pipeline execution
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
