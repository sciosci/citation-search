import requests
import json
import os
from requests.auth import HTTPBasicAuth

from semantic_scholar_apis.dataset_release_ids import get_latest_release_id


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
    if response.status_code == 200:
        print(f'Successfully inserted {len(papers)} documents into "{index_name}"')
    else:
        print(f'Error inserting data: {response.content}')


if __name__ == '__main__':
    og_path = "/home/ubuntu/mypetalibrary/semantic-scholar"
    release_id = get_latest_release_id()
    source_path = f"{og_path}/{release_id}/extracted"
    files = os.listdir(source_path)
    for file in files:
        file_path = f"{source_path}/{file}"
        with open(file_path, "r") as f:
            line = f.readline()
            papers = []
            while line:
                line = f.readline()
                line_mod = json.loads(line)
                corpus_id = line_mod.get("corpusid")
                text = line_mod.get("content").get("text")
                papers.append({"corpusid": str(corpus_id), "text": text})
                if len(papers) == 500:
                    bulk_upload_papers(papers)
                    papers = []
            bulk_upload_papers(papers)
