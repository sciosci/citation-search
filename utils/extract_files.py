"""
This file aims to decompress files in the latest release id
"""
import os
import subprocess
from pathlib import Path
from semantic_scholar_apis.dataset_release_ids import get_latest_release_id


def extract_file(input_file: str, output_file: str):
    # Get the uncompressed file size
    print(f"File {input_file} extraction started.")
    subprocess.run(['gzip', '-c', input_file, '>', output_file], shell=True)
    print(f"File {input_file} decompressed successfully.")


if __name__ == '__main__':
    og_path = "/home/ubuntu/mypetalibrary/semantic-scholar"
    release_id = get_latest_release_id()
    source_path = f"{og_path}/{release_id}/compressed"
    destination_path = f"{og_path}/{release_id}/extracted"
    Path(destination_path).mkdir(parents=True, exist_ok=True)
    files = os.listdir(source_path)

    for file in files:
        inp_file = f"{source_path}/{file}"
        mod_file = file.split(".")[0]
        out_file = f"{destination_path}/{mod_file}"
        extract_file(input_file=inp_file, output_file=out_file)
