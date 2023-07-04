import requests
import os
import urllib.request
from tqdm import tqdm
from pathlib import Path


def get_download_links_of_papers(release_id: str, dataset: str):
    url = f'https://api.semanticscholar.org/datasets/v1/release/{release_id}/dataset/{dataset}/'
    key = os.environ.get('SemanticScholarApiKey')
    headers = {'x-api-key': key}
    response = requests.get(url, headers=headers)
    datasets = response.json()
    links = datasets.get('files')
    return links


def download_file(url, destination):
    with tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=destination) as progress:
        urllib.request.urlretrieve(url, destination, reporthook=progress_hook(progress))


def progress_hook(progress):
    def hook(block_num, block_size, total_size):
        if block_num == 0:
            progress.total = total_size
        downloaded = block_num * block_size
        progress.update(downloaded - progress.n)

    return hook


if __name__ == '__main__':
    release_id = '2023-05-16'
    dataset = 'abstracts'  # ["s2orc", "papers", "abstracts", "authors", "citations"]
    links = get_download_links_of_papers(release_id=release_id, dataset=dataset)
    peta_lib_dir = f"/home/ubuntu/mypetalibrary/semantic-scholar/{release_id}/{dataset}/compressed"
    Path(peta_lib_dir).mkdir(parents=True, exist_ok=True)
    for idx, link in enumerate(links):
        download_file(url=link, destination=f"{peta_lib_dir}/file_{idx + 1}.gz")
