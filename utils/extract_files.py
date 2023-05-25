"""
This file aims to decompress files in the latest release id
"""
import os
import gzip
from pathlib import Path
from semantic_scholar_apis.dataset_release_ids import get_latest_release_id
from tqdm import tqdm


def extract_file(input_file: str, output_file: str):
    # Get the uncompressed file size
    with gzip.open(input_file, 'rb') as f:
        uncompressed_size = f.read()

    # Create a progress bar with the uncompressed size
    with tqdm(total=len(uncompressed_size), unit='B', unit_scale=True, desc='Decompressing', ncols=80) as pbar:
        with gzip.open(input_file, 'rb') as f_in:
            with open(output_file, 'wb') as f_out:
                # Decompress and write to the output file in chunks
                chunk_size = 1024
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    pbar.update(len(chunk))

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
