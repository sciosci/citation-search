import requests
from pprint import pprint
from semantic_scholar_apis.dataset_release_ids import get_latest_release_id


def get_list_of_datasets(release_id: str):
    url = f'https://api.semanticscholar.org/datasets/v1/release/{release_id}'
    response = requests.get(url)
    datasets = response.json()
    dataset_labels = [dataset['name'] for dataset in datasets['datasets']]
    return dataset_labels


if __name__ == '__main__':
    release_id = get_latest_release_id()
    labels = get_list_of_datasets(release_id=release_id)

    print(labels)
