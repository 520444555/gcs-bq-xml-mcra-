import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from xml.etree import ElementTree
from apache_beam.io import fileio
from io import StringIO
import json
from apache_beam import pvalue
from google.cloud import storage
from google.cloud import bigquery
from pathlib import Path
import zipfile
from processors import load_table
from processors  import file_data
from resources import table_list
from options import MyOptions
from processors import getbucket
def run(argv=None):
        #intialising pipeline
        my_pipeline_options = PipelineOptions().view_as(MyOptions.MyOptions)
        #list of tables from parser_output file
        parser_outputs=table_list.parser_outputs
        p=beam.Pipeline(options=PipelineOptions())
        orders = (p
        | beam.Create([None])
        |'bucket_folder'>> beam.ParDo(getbucket.OutputValueProviderFn(my_pipeline_options.bucket_folder)))
        #reading from the bucket
        new_orders=(orders|'Read file' >> fileio.MatchAll()
        |'Read Matches' >> fileio.ReadMatches()
        |'Compute Files' >> beam.ParDo(file_data.file_data(),my_pipeline_options.logging_mode)
        | 'Reshuffle' >> beam.Reshuffle()
        | 'yield_tag' >> beam.ParDo(load_table.load_table(),my_pipeline_options.logging_mode).with_outputs(*parser_outputs)
        )
        #loading xml tag data specific tables 
        for output in parser_outputs:        
		new_orders[output]|"write {0}".format(output) >> beam.io.WriteToBigQuery(my_pipeline_options.dataset+".{0}".format(output),ignore_unknown_columns=True,create_disposition='CREATE_NEVER',write_disposition='WRITE_APPEND',method='STREAMING_INSERTS',insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR')
        result = p.run()
        result.wait_until_finish()
        return result.state
                
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    jobstatus=run()
    print(jobstatus)
