import subprocess

def count_csv_rows(file_path) -> bool:
    result = subprocess.run(f'wc -l {file_path}'.split(' '), capture_output=True, text=True)
    rec_count = int(result.stdout.split()[0]) - 1
    return (rec_count == 100, rec_count)

