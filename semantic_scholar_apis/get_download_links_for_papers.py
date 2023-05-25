import requests
import os
from semantic_scholar_apis.dataset_release_ids import get_latest_release_id
from pprint import pprint


def get_download_links_of_papers(release_id: str):
    url = f'https://api.semanticscholar.org/datasets/v1/release/{release_id}/dataset/s2orc/'
    key = os.environ.get('semantic-scholar-api-key')
    headers = {'x-api-key': key}
    response = requests.get(url, headers=headers)
    datasets = response.json()
    links = datasets.get('files')
    return links


if __name__ == '__main__':
    release_id = get_latest_release_id()
    links = get_download_links_of_papers(release_id=release_id)
    pprint(links)
