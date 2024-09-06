import re, io
import pandas as pd

def process_csv(input_file) -> None:
    """Takes a file with a bad record count, and fixes it to be 100 records"""
    
    match_value = 'snapshot_time(UTC)'
    regex_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s'

    with open(input_file, 'r') as f:
        content = f.read().replace('\n', ' ')

    if match_value:
        content = content.replace(match_value, f'{match_value}\n')
    
    if regex_pattern:
        content = re.sub(regex_pattern, r'\1\n', content)
    
    mock_csv = io.StringIO(content)
    df = pd.read_csv(mock_csv)
    df.to_csv(input_file, index=False)