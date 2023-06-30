import requests
import json
import os
from requests.auth import HTTPBasicAuth
from functools import wraps
from time import time
import multiprocessing


def bulk_upload_papers(papers):
    os_url = 'http://localhost:9200/'
    index_name = 'papers_index'

    # Define the Elasticsearch bulk insert payload
    bulk_data = ''
    for doc in papers:
        bulk_data += f'{{"index":{{"_index":"{index_name}"}}}}\n'
        bulk_data += f'{json.dumps(doc)}\n'

    headers = {'Content-Type': 'application/json'}

    # Insert the data using the OpenSearch bulk API
    response = requests.post(f'{os_url}_bulk', auth=HTTPBasicAuth('admin', 'admin'),
                             headers=headers, data=bulk_data)

    # Check the response status code
    if response.status_code != 200:
        # print(f'Successfully inserted {len(papers)} documents into "{index_name}"')
        # else:
        print(f'Error inserting data: {response.content}')


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r args:[%r, %r] took: %2.4f sec' % \
              (f.__name__, args, kw, te - ts))
        return result

    return wrap


@timing
def file_processor(file_name: str):
    og_path = "/home/ubuntu/mypetalibrary/semantic-scholar"
    release_id = '2023-05-16'
    source_path = f"{og_path}/{release_id}/extracted"
    file_path = f"{source_path}/{file_name}"
    print(f"file {file_name} processing started")
    with open(file_path, "r") as f:
        line = f.readline()
        papers = []
        while line:
            line_mod = json.loads(line)
            corpus_id = line_mod.get("corpusid")
            text = line_mod.get("content").get("text")
            papers.append({"corpusid": str(corpus_id), "text": text})
            if len(papers) == 500:
                bulk_upload_papers(papers)
                papers = []
            line = f.readline()
        if papers:
            bulk_upload_papers(papers)
    print(f"file {file_name} processing completed")


if __name__ == '__main__':
    og_path = "/home/ubuntu/mypetalibrary/semantic-scholar"
    release_id = '2023-05-16'
    source_path = f"{og_path}/{release_id}/extracted"
    files = os.listdir(source_path)
    # for file in files:
    #     file_processor(file_name=file)
    with multiprocessing.Pool(processes=8) as pool:
        results = pool.map(file_processor, files)
