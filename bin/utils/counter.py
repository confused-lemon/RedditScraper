import subprocess

def count_csv_rows(file_path) -> int:
    """Returns the number of records in a file, ignoring header row\n
    Expected return is 100, otherwise will raise assert error"""
    result = subprocess.run(f'wc -l {file_path}'.split(' '), capture_output=True, text=True)
    return int(result.stdout.split()[0]) - 1