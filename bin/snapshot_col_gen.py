import pandas as pd
import os
from datetime import datetime

path_to_file = os.path.dirname(os.path.dirname(__file__))

file_list = [file for file in os.listdir(f'{path_to_file}/CSV_datasets')]
path = '/home/lighthouse/Desktop/Code/Projects/RedditScraper/CSV_datasets/20230623_211502.csv'

# for file in file_list:
#     stamp = file.replace('.csv', '')
df = pd.read_csv(path)
headers = df.columns.tolist()
file = '20230623_211502.csv'

if 'snapshot_time(UTC)' in headers:
    pass
else:
    stamp = file.replace('.csv', '')
    datetime_obj = datetime.strptime(stamp, "%Y%m%d_%H%M%S")
    new_stamp = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    df["snapshot_time(UTC)"] = new_stamp
    # df.to_csv(,index=False)