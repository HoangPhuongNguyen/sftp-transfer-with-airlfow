from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from libs.sftp_transfer import intergrate_ftp_server
from configs.ftps import FTPSource, FTPTarget

dag = DAG(
    'sftp_transfer_dag',
    description='Test XCOM in airflow for tasking  in a DAG',
    # schedule='*/2 * * * *',
    start_date=datetime(2024, 6, 1)
)

check_folder_ftp = PythonOperator(
    task_id='check_folder_ftp',
    dag=dag,
    python_callable=intergrate_ftp_server,
    op_kwargs={
        'source': FTPSource,
        'target': FTPTarget,
    },
    provide_context=True
)

check_folder_ftp
