"""
This file aims to decompress files in the latest release id
"""
import os
import subprocess
from pathlib import Path


def extract_file(input_file: str, output_file: str):
    # Get the uncompressed file size
    print(f"File {input_file} extraction started.")
    command = ['gzip', '-dk', input_file, output_file]
    process = subprocess.run(command, capture_output=True)
    print("Return Code: ", process.returncode)
    print("Stdout: ", process.stdout)
    print(f"File {input_file} decompressed successfully.")


if __name__ == '__main__':
    release_id = '2023-05-16'
    dataset = 'citations'  # ["s2orc", "papers", "abstracts", "authors", "citations"]
    source_path = f"/home/ubuntu/mypetalibrary/semantic-scholar/{release_id}/{dataset}/compressed"
    destination_path = f"/home/ubuntu/mypetalibrary/semantic-scholar/{release_id}/{dataset}/extracted"
    Path(destination_path).mkdir(parents=True, exist_ok=True)
    files = os.listdir(source_path)
    for file in files:
        inp_file = f"{source_path}/{file}"
        mod_file = file.split(".")[0]
        out_file = f"{destination_path}/{mod_file}"
        extract_file(input_file=inp_file, output_file=out_file)