import apache_beam as beam
from io import StringIO
import json
from apache_beam import pvalue
from resources import table_list
import logging
 
class load_table(beam.DoFn):
    def process(self,xmlfile,logging_mode):
        #adding logs
        logging.getLogger().setLevel(logging.getLevelName(logging_mode))
        from apache_beam import pvalue
        import xmltodict
        batch_date=xmlfile[0].rsplit("/",1)[-1]
        batch_date=batch_date[10:18]
        batch_date=batch_date[:4]+"-"+ batch_date[4:6]+"-"+batch_date[6:]
        logging.info('batch date ---- %s',batch_date)
        #converting xml to dict
	xmldata=xmlfile[1].decode("utf-8").replace('\n','')
        data = xmltodict.parse(xmldata,attr_prefix='',strip_whitespace=False)
        #getting list of tables
        tables=table_list.tables_tag
        json_data=json.dumps(data['CreditReport'])
        json_data2=json.loads(json_data)
        logging.debug('xml data -----%s',json_data2)
        #seperating header tag from the xml dict
        if 'HeaderResponseSegment' not in json_data2.keys():
            for key in json_data2:
                if isinstance(json_data2[key], dict):
                    for idx in json_data2[key]:
                        if isinstance(json_data2[key][idx],list):
			   for i in json_data2[key][idx]:
                                i['BATCH_DATE']=batch_date
                                table_name=tables.get(idx)
                                #adding logs
                                logging.info('table name emittied ---- %s',table_name)
                                yield pvalue.TaggedOutput(table_name,i)
                        else:
                            if isinstance(json_data2[key][idx],dict):
                                json_data2[key][idx]['BATCH_DATE']=batch_date
                                table_name=tables.get(idx)
                                #adding logs
                                logging.info('table name emittied ---- %s',table_name)
                                yield pvalue.TaggedOutput(table_name,json_data2[key][idx])
                                
		else:
                    for idx in json_data2[key]:
                        if isinstance(idx,list):
                            for i in json_data2[key][idx]:
                                print('1')
                        else:
                            if isinstance(idx,dict):
                                for x in idx:
                                    if isinstance(idx[x],dict):
                                        idx[x]['BATCH_DATE']=batch_date
                                        table_name=tables.get(x)
                                        #adding logs
                                        logging.info('table name emittied ---- %s',table_name)
                                        yield pvalue.TaggedOutput(table_name,idx[x])
else:
                                        for y in idx[x]:
                                            y['BATCH_DATE']=batch_date
                                            table_name=tables.get(x)
                                            #adding logs
                                            logging.info('table name emittied ---- %s',table_name)
                                            yield pvalue.TaggedOutput(table_name,y)            
            return
        header=json_data2.pop('HeaderResponseSegment')
        header['HeaderSegmentTag']=header.pop('SegmentTag')
        header['BATCH_DATE']=batch_date
        logging.info('Header ----%s',header)
        yield pvalue.TaggedOutput('MCRA_TUEF_SEGMENT_HEADER',header)
        #code to extract all the xml tags and tagging with table name for bq load
        for key in json_data2:
            if isinstance(json_data2[key], dict):
                for idx in json_data2[key]:
			if isinstance(json_data2[key][idx],list):
                        for i in json_data2[key][idx]:
                            z={}
                            z['BATCH_DATE']=batch_date
                            z={**header,**i}
                            table_name=tables.get(idx)
                            #adding logs
                            logging.info('table name emittied ---- %s',table_name)
                            yield pvalue.TaggedOutput(table_name,z)
                    else:
                        if isinstance(json_data2[key][idx],dict):
                            z={}
                            z['BATCH_DATE']=batch_date
                            z={**header,**json_data2[key][idx]}
                            table_name=tables.get(idx)
                            #adding logs
                            logging.info('table name emittied ---- %s',table_name)
                            yield pvalue.TaggedOutput(table_name,z)
            else:
		if isinstance(json_data2[key][idx],dict):
                            z={}
                            z['BATCH_DATE']=batch_date
                            z={**header,**json_data2[key][idx]}
                            table_name=tables.get(idx)
                            #adding logs
                            logging.info('table name emittied ---- %s',table_name)
                            yield pvalue.TaggedOutput(table_name,z)
            else:
                for idx in json_data2[key]:
                    if isinstance(idx,list):
                        for i in json_data2[key][idx]:
                            print('1')
                    else:
                        if isinstance(idx,dict):
                            for x in idx:
		if isinstance(idx[x],dict):
                                    z={}
                                    z['BATCH_DATE']=batch_date
                                    z={**header,**idx[x]}
                                    table_name=tables.get(x)
                                    #adding logs
                                    logging.info('table name emittied ---- %s',table_name)
                                    yield pvalue.TaggedOutput(table_name,z)
                                else:
                                    for y in idx[x]:
                                        z={}

                                        z['BATCH_DATE']=batch_date
                                        z={**header,**y}
                                        table_name=tables.get(x)
                                        #adding logs
                                        logging.info('table name emittied ---- %s',table_name)
                                        yield pvalue.TaggedOutput(table_name,z)
