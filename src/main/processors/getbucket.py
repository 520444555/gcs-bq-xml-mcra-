import apache_beam as beam
 
class OutputValueProviderFn(beam.DoFn):
  def __init__(self, vp):
    self.vp = vp
 
  def process(self, unused_elm):
    #getting the bucket name in runtime
    from apache_beam import pvalue
    yield "gs://"+self.vp.get()+"/*.zip"

