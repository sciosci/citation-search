import json
import requests
import os
from opensearchpy import OpenSearch, RequestsHttpConnection


def get_latest_release_id():
    url = 'https://api.semanticscholar.org/datasets/v1/release'
    response = requests.get(url)
    ids = response.json()
    latest_id = ids[-1]
    return latest_id


def bulk_upload_papers(open_search_client, papers):
    # Define the Elasticsearch bulk insert payload
    bulk_data = ''
    for doc in papers:
        bulk_data += f'{{"index":{{"_index":"{index_name}"}}}}\n'
        bulk_data += f'{json.dumps(doc)}\n'

    response = open_search_client.bulk(papers)

    print(response)


if __name__ == '__main__':
    og_path = "/home/ubuntu/mypetalibrary/semantic-scholar"
    release_id = get_latest_release_id()
    source_path = f"{og_path}/{release_id}/extracted"
    files = os.listdir(source_path)

    host = 'localhost'
    port = 9200
    auth = ('admin', 'admin')

    ca_certs_path = '/etc/opensearch/root-ca.pem'

    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_compress=True,  # enables gzip compression for request bodies
        http_auth=auth,
        connection_class=RequestsHttpConnection,
        # client_cert = client_cert_path,
        # client_key = client_key_path,
        use_ssl=True,
        verify_certs=True,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        ca_certs=ca_certs_path
    )

    index_name = 'papers_index'

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
