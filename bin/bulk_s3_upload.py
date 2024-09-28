import boto3, os, sys, yaml, glob

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
path = os.path.dirname(os.path.dirname(__file__))

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

for file in history_files:
    file_name = file.split('/')[-1]
    year, month,day = file_name[0:4], file_name[4:6], file_name[6:8]
    s3_session.upload_file(file, s3_bucket, f'{year}/{month}/{day}/{file_name}')