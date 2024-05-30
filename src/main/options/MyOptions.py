import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
 
class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_value_provider_argument('--bucket_folder',default='fsmiasp-dataproc-user-hrushabh/xml_files/test_five')
    parser.add_argument('--logging_mode',default='INFO')
    parser.add_argument('--dataset')
