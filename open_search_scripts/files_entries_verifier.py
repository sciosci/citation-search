import os
import json

if __name__ == '__main__':
    if __name__ == '__main__':
        og_path = "/home/ubuntu/mypetalibrary/semantic-scholar"
        release_id = '2023-05-16'
        source_path = f"{og_path}/{release_id}/extracted"
        files = os.listdir(source_path)
        final_ctr = 0
        for file in files:
            file_path = f"{source_path}/{file}"
            with open(file_path, "r") as f:
                line = f.readline()
                papers = []
                while line:
                    line = f.readline()

