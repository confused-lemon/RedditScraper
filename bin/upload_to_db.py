import yaml, os, glob, sys, logging.config, base64, pandas, boto3
import subprocess as sp
from pyspark.sql import SparkSession
from schema import TABLE_SCHEMA
from utils import counter, count_fixer
from datetime import datetime
from botocore.exceptions import ClientError

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

    s3_bucket = credentials['AWS']['bucket']
    aws_access_key = credentials['AWS']['access_key']
    aws_secret_key = credentials['AWS']['secret_key']

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Daily Record Upload") \
    .config('spark.jars', '/usr/local/bin/postgresql-42.7.3.jar') \
    .getOrCreate()

connection = {
    "user" : username,
    "password" : password,
    "driver" : "org.postgresql.Driver"
}

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    )

files = glob.glob(f'{path}/CSV_datasets/*.csv')

def decode_from_base64(encoded_string: str) -> str:
    base64_bytes = encoded_string.encode('utf-8')
    byte_data = base64.b64decode(base64_bytes)
    return byte_data.decode('utf-8')

def conv_str_to_ts(input_time_str: str) -> datetime:
    return datetime.strptime(input_time_str, "%Y-%m-%d %H:%M:%S")

def upload_local(current_file) -> None:
    df = pandas.read_csv(current_file)

    df['title'] = df['title'].apply(lambda title: decode_from_base64(title))
    df['author_flair_text'] = df['author_flair_text'].apply(lambda flair: decode_from_base64(flair) if pandas.notnull(flair) else flair)
    df['snapshot_time(UTC)'] = df['snapshot_time(UTC)'].apply(lambda snapshot_time: conv_str_to_ts(snapshot_time))
    df_spark = spark.createDataFrame(df, schema=TABLE_SCHEMA)
    df_spark = df_spark.withColumnRenamed("snapshot_time(UTC)", "snapshot_time_utc")

    df_spark.write.jdbc(url=f'jdbc:postgresql://{ip_addr}:{port}/{db}', table=main_table, properties=connection, mode='append')

def upload_aws(datafile) -> None:
    file_name = datafile.split('/')[-1]
    year, month,day = file_name[0:4], file_name[4:6], file_name[6:8]
    s3.upload_file(datafile, s3_bucket, f'{year}/{month}/{day}/{file_name}')

for data_file in files: 
    try:
        count = counter.count_csv_rows(data_file)
        assert count == 100
        upload_local(data_file)
        # upload_aws(data_file)
        sp.run(['mv', f'{data_file}', f'{path}/Processed_files/'], check=True)
        
    except AssertionError:
        try:
            count_fixer.process_csv(data_file)
            assert counter.count_csv_rows(data_file) == 100
            upload_local(data_file)
            upload_aws(data_file)
            sp.run(['mv', f'{data_file}', f'{path}/Processed_files/'], check=True)

        except AssertionError:
            sp.run(['mv', f'{data_file}', f'{path}/Error_Files/counts'], check=True)
            logging.error(f"File {data_file.split('/')[-1]} has a record count error({count})")
    except ClientError as auth_error:
        logging.critical(f'An error occured with the credentials provided:\n{auth_error}')
    except Exception as sqlException:
        sp.run(['mv', f'{data_file}', f'{path}/Error_Files/Exception'], check=True)
        logging.error(f"Error occured when attempting to upload file: {data_file}\n{sqlException}")