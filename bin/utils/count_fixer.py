import re, io
from glob import glob
import pandas as pd
from counter import count_csv_rows

def process_csv(input_file, output_file, match_value=None, regex_pattern=None):
    with open(input_file, 'r') as f:
        content = f.read().replace('\n', ' ')

    if match_value:
        content = content.replace(match_value, f'{match_value}\n')
    
    if regex_pattern:
        content = re.sub(f'({regex_pattern})', r'\1\n', content)
    
    mock_csv = io.StringIO(content)
    df = pd.read_csv(mock_csv)
    df.to_csv(output_file, index=False)

files = glob('Error_Files/counts/*.csv')
for file in files:
    input_file = file
    output_file = file
    match_value = 'snapshot_time(UTC)'
    regex_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'

    process_csv(input_file, output_file, match_value, regex_pattern)

    try:
        assert count_csv_rows(output_file) == 100
    except AssertionError:
        print(f'{output_file} needs attention')