import json
from collections import defaultdict
from pprint import pprint

with open('../../temp/pmoa_json_format_reader/sample.json', 'r') as f:
    data = json.load(f)
list_of_dict = json.loads(data)

relevant_keys = ['pmid', 'paraid', 'sentid', 'sentence', 'citations']

filtered_list = []
all_cited = []
paragraph_pairs = defaultdict(list)

for tmp_dict in list_of_dict:
    if tmp_dict['has_citations'] and tmp_dict['citations']:
        curr_dict = {key: tmp_dict[key] for key in relevant_keys}
        filtered_list.append(curr_dict)
        for cited_id in curr_dict['citations']:
            all_cited.append(cited_id)
            paragraph_pairs[curr_dict['paraid']].append(cited_id)

# Relevant data structure to support the creation of final dataset

# pprint(filtered_list)
# pprint(all_cited)
# pprint(paragraph_pairs)

# Creation of final dataset
perfectly_relevant_list = []
keys = ['pmid', 'paraid', 'sentid', 'sentence']
# 1. Add perfectly relevant data points:
for entry in filtered_list:
    for cited_id in entry['citations']:
        new_entry = {}
        for key in keys:
            new_entry[key] = entry[key]
        new_entry['cited_id'] = cited_id
        new_entry['relevance_score'] = 3
        perfectly_relevant_list.append(new_entry)

pprint(perfectly_relevant_list)

# 2. Add somewhat relevant data points :
somewhat_relevant_list = []


# 3. Add slightly relevant data points :
slightly_relevant_list = []

# 4. Add irrelevant data points :
irrelevant_list = []
