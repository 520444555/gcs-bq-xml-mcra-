from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator,ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStoragePrefixSensor
from random import randint
import time
args={'start_date':days_ago(1),'owner':'IHUBHK_MCRA',}
 
dag=DAG(
        dag_id='IHUBHK_MCRA_AMH',
        default_args=args,
        access_control={
                'IHUBHK_MCRA_editor':{'can_dag_edit'},
                'IHUBHK_MCRA_reader':{'can_dag_read'},
        },
        schedule_interval='0 1,4,7,10,13,16 * * *'
        )
 
 
def file_status(**kwargs):
        from google.cloud import storage
 
        #name = 'batch/FCRPTCRATU'
        prefix= 'FCRPTCRATU'
        storage_client = storage.Client()
        bucket_name = 'df-11546216-mcraamh-prod'
        bucket = storage_client.bucket(bucket_name)
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix,delimiter='/')
        files=[]
        for blob in blobs:
        
		files.append(blob.name)
        if len(files) > 0:
                print("******Files Present****")
                return True
        else:
                print("******Files Not Present****")
                return False
 
 
 
archive_cmd=''' echo $bucketfolder;
                echo $jobid;
                archive_zip() { 
                        job_id=$1 ; 
                        bucket_folder=$2 ;
                        echo $job_id ; 
                        echo $bucket_folder ; 
                        status=`gcloud dataflow jobs list --filter="${job_id}" --format="get(state)" --region=asia-east2 --project hsbc-10436866-dfasp-prod`;
                        echo $status;
			if [[ $status == "Done" ]]; 
                        then 
                        move_status=`gsutil -m cp gs://${bucket_folder}/*.zip gs://df-11546216-mcraamh-prod/completed/ ; gsutil -m rm -rf gs://${bucket_folder}` ; 
                        elif [[ $status == "Failed" ]]; 
                        then 
                        move_status=`gsutil -m cp gs://${bucket_folder}/*.zip gs://df-11546216-mcraamh-prod/error/ ; gsutil -m rm -rf gs://${bucket_folder}`; 
                        else 
                        sleep 120 && archive_zip $job_id $bucket_folder ; 
                        fi;
                 };
                 archive_zip $jobid $bucketfolder'''
                 
dataflow_cmd='''echo $bucketfolder;
              gcloud dataflow jobs run ihubmcra_$bucketfolder --gcs-location gs://df-11546216-mcraamh-prod/templates/gcs-bq-ihubmcra --parameters bucket_folder=$bucketfolder --region asia-east2 --project hsbc-10436866-dfasp-prod --staging-location=gs://df-11546216-mcraamh-prod/pipelines/gcs-bq-ihubmcra/temp | grep "id" | cut -f2 -d ":" '''
 
 
gcloud_config=BashOperator(
        task_id='gcloud_config',
        bash_command='gcloud config list',
        dag=dag
)
 
filewatcher = ShortCircuitOperator(
        task_id='filewatcher',
        python_callable=file_status,
        op_kwargs={},
        dag=dag
        )
                
 
copy=BashOperator(
        task_id='copy',
        bash_command='d=`date +"%d%m%Y_%H%M%S"`;gsutil -m mv gs://df-11546216-mcraamh-prod/FCRPTCRATU*.zip gs://df-11546216-mcraamh-prod/processing/$d/;echo "df-11546216-mcraamh-prod/processing/$d"',
        xcom_push=True,
        dag=dag
)                 


Dataflow_trigger = BashOperator(
        task_id='Dataflow-trigger',
        bash_command=dataflow_cmd,
        env={"bucketfolder":"{{ ti.xcom_pull(task_ids='copy')}}"},
        xcom_push=True,
        dag=dag
        )
 
sleep = PythonOperator(task_id="sleep",dag=dag,python_callable=lambda: time.sleep(180))
 
archival = BashOperator(
        task_id='archival',
        bash_command=archive_cmd,
        env={"bucketfolder":"{{ ti.xcom_pull(task_ids='copy')}}","jobid":"{{ ti.xcom_pull(task_ids='Dataflow-trigger')}}"},
        dag=dag
        )
 
 
gcloud_config >> filewatcher >> copy >> Dataflow_trigger >> sleep >> archival
