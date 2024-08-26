import os, sys
import pandas as pd
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root); path = os.path.dirname(os.path.dirname(__file__))


frames =[]

for csv_file in set(os.listdir(f'{path}/CSV_datasets')):
    file = os.path.join(path, f'CSV_datasets/{csv_file}')
    df = pd.read_csv(file, skiprows=1)
    frames.append(df)

full_frame = pd.concat(frames, ignore_index=True)
print(f"Numer of records: {full_frame.size}")
print(f"Unique posts (based on post id): {full_frame['id'].nunique()}")
