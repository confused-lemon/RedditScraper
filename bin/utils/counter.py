import subprocess

def count_csv_rows(file_path) -> int:
    result = subprocess.run(f'wc -l {file_path}'.split(' '), capture_output=True, text=True)
    return int(result.stdout.split()[0]) - 1