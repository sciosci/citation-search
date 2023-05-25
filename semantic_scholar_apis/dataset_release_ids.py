import requests


def get_latest_release_id():
    url = 'https://api.semanticscholar.org/datasets/v1/release'
    response = requests.get(url)
    ids = response.json()
    latest_id = ids[-1]
    return latest_id


if __name__ == '__main__':
    print(get_latest_release_id())
