import requests
import json


def get_cids(idx):
    endpoint = 'http://127.0.0.1:6001/corpus_ids'
    params = {'idx': idx}
    response = requests.get(endpoint, params=params)
    response_dict = json.loads(response.text)
    cid = response_dict.get('cid')
    return cid


print(get_cids(idx=5))
