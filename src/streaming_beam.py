import os
import datetime
from datetime import timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/guilhermelana/Documents/fiap/natenv/natenv-055a2ca6fbee.json'

input_subscription = 'projects/natenv/subscriptions/apache-beam'

output = 'projects/natenv/topics/mail_output'

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

pipe = beam.Pipeline(options=options)

def custom_timestamp(element):
    dt = datetime.datetime.strptime(element[0].replace(' UTC', ""), "%Y-%m-%d %H:%M:%S")
    timestamp = dt.replace(tzinfo=timezone.utc).timestamp()
    return beam.window.TimestampedValue(element, int(timestamp))

def encode_byte_string(element):
   print(element)
   element = str(element)
   return element.encode('utf-8')

ecommerce_data = (
        pipe
        | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
        | "Decode utf-8" >> beam.Map(lambda row: row.decode('utf-8'))
        | "Filter Nones" >> beam.Filter(lambda row: (type(row)!=None))
        | "Spliting rows" >> beam.Map(lambda row: row.split(","))
        | "Filter by cart without purchase" >> beam.Filter(lambda row: (row[1] == 'cart'))
        | "Apply custom_timestamp" >> beam.Map(custom_timestamp)
        | "Map to session" >> beam.Map(lambda row: (row[8].replace("\n", ""), float(row[6])))
        | "Window" >> beam.WindowInto(beam.window.Sessions(15))
        | "Combine per key - sum" >> beam.CombinePerKey(sum)
        | "Encode to byte string" >> beam.Map(encode_byte_string)
        | "Write to output" >> beam.io.WriteToPubSub(topic=output)
    )

results = pipe.run()
results.wait_until_finish()
