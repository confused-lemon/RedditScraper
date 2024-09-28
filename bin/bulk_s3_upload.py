import boto3, os, sys, yaml, glob, logging, logging.config
import subprocess as sp

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
path = os.path.dirname(os.path.dirname(__file__))
from cfg.logging_config import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
logging.getLogger('history_logger')

with open(f'{path}/cfg/credentials.yaml','r') as cred_file:
    credentials = yaml.safe_load(cred_file)

    s3_bucket = credentials['AWS']['bucket']
    aws_access_key = credentials['AWS']['access_key']
    aws_secret_key = credentials['AWS']['secret_key']

s3_session = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    )

history_files = glob.glob('Processed_files/*.csv')
print(f'Files to upload: {len(history_files)}')
errs =[]
err_count=0
for file in history_files:
    try:
        file_name = file.split('/')[-1]
        year, month,day = file_name[0:4], file_name[4:6], file_name[6:8]
        s3_session.upload_file(file, s3_bucket, f'{year}/{month}/{day}/{file_name}')
    except Exception as e: 
        err_count += 1
        logging.error(f'Failed to upload {file_name}. Reason: {e}')
        sp.run(['mv', f'{file}', f'{path}/Error_Files/History_upload'], check=True)


print(f'Errors: {err_count}')