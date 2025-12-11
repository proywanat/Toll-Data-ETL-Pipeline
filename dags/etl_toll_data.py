from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
import os
import pendulum

# defining DAG arguments
default_args = {
    'owner': 'proywanat',
    'start_date': pendulum.now('UTC').subtract(days=0),
    'email': ['proywanat@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

# defining the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Project',
    schedule=timedelta(days=1),  
)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATASET_PATH = os.path.join(BASE_PATH, 'dataset', 'tolldata.tgz')

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzvf {DATASET_PATH} -C {os.path.join(BASE_PATH, "dataset")}',
    dag=dag,
)

VEHICLE_DATA = os.path.join(BASE_PATH, 'dataset', 'vehicle-data.csv')
TOLLPLAZA_DATA = os.path.join(BASE_PATH, 'dataset', 'tollplaza-data.tsv')
PAYMENT_DATA = os.path.join(BASE_PATH, 'dataset', 'payment-data.txt')
EXTRACTED_DATA = os.path.join(BASE_PATH, 'dataset', 'extracted_data.csv')

# Extract from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f'cut -d"," -f1-4 < {VEHICLE_DATA} > {os.path.join(BASE_PATH, "dataset", "csv_data.csv")}',
    dag=dag,
)

# Extract from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f'cut -f5-7 < {TOLLPLAZA_DATA} > {os.path.join(BASE_PATH, "dataset", "tsv_data.csv")}',
    dag=dag,
)

# Extract from fixed-width TXT
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f'cut -c59-68 < {PAYMENT_DATA} > {os.path.join(BASE_PATH, "dataset", "fixed_width_data.csv")}',
    dag=dag,
)

# Consolidate all extracted data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        f'paste {os.path.join(BASE_PATH, "dataset", "csv_data.csv")} '
        f'{os.path.join(BASE_PATH, "dataset", "tsv_data.csv")} '
        f'{os.path.join(BASE_PATH, "dataset", "fixed_width_data.csv")} '
        f'> {EXTRACTED_DATA}'
    ),
    dag=dag,
)

# Transform data (convert to uppercase)
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'tr "[a-z]" "[A-Z]" < {EXTRACTED_DATA} > {EXTRACTED_DATA}',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data






