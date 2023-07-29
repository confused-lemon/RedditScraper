import pandas as pd
import os
from datetime import datetime

path_to_file = os.path.dirname(os.path.dirname(__file__))

file_list = [file for file in os.listdir(f'{path_to_file}/CSV_datasets')]
print(f'Found {len(file_list)} files for review')
changed = 0
skipped = 0
for file in file_list:
    fileloc = f'{path_to_file}/CSV_datasets/{file}'
    df = pd.read_csv(fileloc)
    headers = df.columns.tolist()
    if 'snapshot_time(UTC)' in headers:
        skipped += 1
        continue
    else:
        changed += 1
        stamp = file.replace('.csv', '')
        datetime_obj = datetime.strptime(stamp, "%Y%m%d_%H%M%S")
        new_stamp = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
        df["snapshot_time(UTC)"] = new_stamp
        df.to_csv(f'{path_to_file}/CSV_datasets/{stamp}.csv',index=False)

print(f'Changed {changed} files')
print(f'Skipped {skipped} files')