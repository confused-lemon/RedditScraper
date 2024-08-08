import yaml, os, glob, sys, logging.config
import subprocess as sp
from pyspark.sql import SparkSession
from schema import TABLE_SCHEMA
from counter import count_csv_rows

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
from cfg.logging_config import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)

path = os.path.dirname(os.path.dirname(__file__))
logging.getLogger('uploader')

with open(f'{path}/cfg/credentials.yaml','r') as cred_file:
    credentials = yaml.safe_load(cred_file)

    ip_addr = credentials['Database']['ip_addr']
    port = credentials['Database']['port']
    username = credentials['Database']['username']
    db = credentials['Database']['database']
    main_table = credentials['Database']['main_table']
    password = credentials['Database']['password']

spark = SparkSession.builder \
    .master("local") \
    .appName("Test Connection") \
    .config('spark.jars', '/usr/local/bin/postgresql-42.7.3.jar') \
    .getOrCreate()

connection = {
    "user" : username,
    "password" : password,
    "driver" : "org.postgresql.Driver"
}

files = glob.glob(f'{path}/CSV_datasets/*.csv')

count = 0
for data_file in files:
    try:
        flag, count = count_csv_rows(data_file)
        assert flag
        
        df = spark.read \
        .option("quote", "\'") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(data_file, header=True,  schema=TABLE_SCHEMA)

        df = df.withColumnRenamed("snapshot_time(UTC)", "snapshot_time_utc")
        df.write.jdbc(url=f'jdbc:postgresql://{ip_addr}:{port}/{db}', table= main_table, properties = connection, mode='append')
        sp.run(['mv', f'{data_file}', f'{path}/Processed_files/'], check=True)
        
        if count == 100:
            break
        else:
            count+=1
    except AssertionError as ae:
        sp.run(['mv', f'{data_file}', f'{path}/Error_Files/counts'], check=True)
        logging.error(f"File {data_file.split('/')[-1]} has a record count error({count_csv_rows})")
    except Exception as sqlException:
        sp.run(['mv', f'{data_file}', f'{path}/Error_Files/Exception'], check=True)
        logging.error(f"Error occured when attempting to upload file: {data_file}\n{sqlException}")
    