import urllib.request
from tqdm import tqdm

from semantic_scholar_apis.dataset_release_ids import get_latest_release_id
from semantic_scholar_apis.get_download_links_for_papers import get_download_links_of_papers


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
    release_id = get_latest_release_id()
    links = get_download_links_of_papers(release_id=release_id)
    peta_lib_dir = f"/home/ubuntu/mypetalibrary/semantic-scholar/{release_id}/compressed"
    for idx, link in enumerate(links):
        download_file(url=link, destination=f"file_{idx + 1}.gz")
