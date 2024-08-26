import subprocess

def count_csv_rows(file_path) -> int:
    """Returns the number of records in a file, ignoring header row\n
    Expected return is 100, otherwise will raise value error"""
    result = subprocess.run(f'wc -l {file_path}'.split(' '), capture_output=True, text=True)
    row_count = int(result.stdout.split()[0]) - 1
    if row_count != 100:
        raise ValueError(f'Row count check failed. Expected 100, got {row_count}') 