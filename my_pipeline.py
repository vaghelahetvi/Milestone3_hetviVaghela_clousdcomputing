import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Options clearly set for Dataflow
options = PipelineOptions(
    runner='DataflowRunner',
    project='oceanic-student-451923-m7',
    temp_location='gs://oceanic-student-451923-m7-bucket/temp',
    region='northamerica-northeast2'
)

def run():
    with beam.Pipeline(options=options) as p:
        # Read CSV from bucket
        lines = p | 'ReadInputFile' >> beam.io.ReadFromText('gs://oceanic-student-451923-m7-bucket/input_data.csv', skip_header_lines=1)

        # Filter: score above 75
        filtered = lines | 'FilterHighScores' >> beam.Filter(lambda line: int(line.split(',')[2]) > 75)

        # Write results
        filtered | 'WriteResults' >> beam.io.WriteToText('gs://oceanic-student-451923-m7-bucket/results/output')

if __name__ == '__main__':
    run()