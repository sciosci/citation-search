import json
import pandas as pd

with open('sample.json', 'r') as f:
    data = json.load(f)
list_of_dict = json.loads(data)

from pprint import pprint

pprint(list_of_dict)


# keys = ['pmid', 'sentence', 'has_citations', 'citations']
#
# bunch = []
#
# for entry in list_of_dict:
#     if entry[keys[2]] and entry[keys[3]]:
#         for cited in entry[keys[3]]:
#             bunch.append([entry[keys[0]], entry[keys[1]], cited])
#
# df = pd.DataFrame(bunch, columns=['PmId', 'Sentence', 'Cited'])
#
# df.to_csv("transformed_data_sample.csv")