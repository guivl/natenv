import apache_beam as beam


pipe = beam.Pipeline()


def encode_byte_string(element):
    print(element)
    element = str(element)
    return element.encode('utf-8')


testPcollection = (
        pipe
        | beam.io.ReadFromText('../data/tmp/testfile.csv')
        | beam.Map(lambda row: row.split(","))
        | beam.Map(lambda row: (row[9], float(row[7])))
        | beam.CombinePerKey(sum)
        #| beam.Map(encode_byte_string)
        | beam.io.WriteToText('../data/output')
)
pipe.run()
