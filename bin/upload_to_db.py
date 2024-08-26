import yaml, os, glob, sys, logging.config, base64, pandas
import subprocess as sp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from schema import TABLE_SCHEMA
from utils.counter import count_csv_rows
from datetime import datetime
from functools import lru_cache

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
    .master("local[*]") \
    .appName("Test Connection") \
    .config('spark.jars', '/usr/local/bin/postgresql-42.7.3.jar') \
    .getOrCreate()

connection = {
    "user" : username,
    "password" : password,
    "driver" : "org.postgresql.Driver"
}

files = glob.glob(f'{path}/CSV_datasets/*.csv')

@lru_cache
def decode_from_base64(encoded_string: str) -> str:
    base64_bytes = encoded_string.encode('utf-8')
    byte_data = base64.b64decode(base64_bytes)
    return byte_data.decode('utf-8')

@lru_cache
def conv_str_to_ts(input_time_str: str) -> datetime:
    return datetime.strptime(input_time_str, "%Y-%m-%d %H:%M:%S")

@lru_cache
def clean_backslash_quotes(input_string: str) -> str:
    return input_string.replace(r'\"', '"').replace(r"\'", "'")

for data_file in files: #first in base64 is 20240818_160002.csv
    try:
        # count = count_csv_rows(data_file)
        # if count != 100:
        #     raise AssertionError

        df = pandas.read_csv(data_file)
        try:
            df['title'] = df['title'].apply(lambda title: decode_from_base64(title))
        except Exception as e:
            df['title'] = df['title'].apply(lambda title: clean_backslash_quotes(title))

        df['snapshot_time(UTC)'] = df['snapshot_time(UTC)'].apply(lambda snapshot_time: conv_str_to_ts(snapshot_time))

        df_spark = spark.createDataFrame(df, schema=TABLE_SCHEMA)

        df_spark = df_spark.withColumnRenamed("snapshot_time(UTC)", "snapshot_time_utc")
        
        # df_spark.write.jdbc(url=f'jdbc:postgresql://{ip_addr}:{port}/{db}', table=main_table, properties=connection, mode='append')
        # sp.run(['mv', f'{data_file}', f'{path}/Processed_files/'], check=True)
    except AssertionError:
        # sp.run(['mv', f'{data_file}', f'{path}/Error_Files/counts'], check=True)
        # logging.error(f"File {data_file.split('/')[-1]} has a record count error({count})")
        raise NotImplementedError
    except Exception as sqlException:
        print(f'{data_file}: {sqlException}')
        # sp.run(['mv', f'{data_file}', f'{path}/Error_Files/Exception'], check=True)
        # logging.error(f"Error occured when attempting to upload file: {data_file}\n{sqlException}")