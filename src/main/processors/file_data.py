import zipfile
import apache_beam as beam
import logging
 
class file_data(beam.DoFn):
        def process(self,text,logging_mode):
                #adding logs
                
                logging.getLogger().setLevel(logging.getLevelName(logging_mode))
                name=text.metadata.path
                logging.info('zip name ----%s',name)
                input_zip=zipfile.ZipFile(text.open())
                
                
                for i in input_zip.namelist():
                        #getting data and file name from zip
 
               
                        return_string=input_zip.read(i)
                        yield name,return_string
