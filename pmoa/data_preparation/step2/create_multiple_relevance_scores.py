import json
import random
import logging
import time
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor
from string import Template
from copy import deepcopy
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, insert

logging.basicConfig(filename='logs/create_multiple_relevance_scores.log', filemode='a',
                    format='%(name)s - %(levelname)s - %(message)s')

conn_params = {
    "host": "10.224.68.29",
    "port": "5432",
    "database": "pubmed",
    "user": "admin",
    "password": "admin"
}

engine = create_engine('postgresql://', connect_args=conn_params)

metadata = MetaData()

relevance_store = Table(
    'relevance_store_new', metadata,
    Column('pmid', String),
    Column('sentence', String),
    Column('cited_id', String),
    Column('relevance_score', Integer),
    Column('paraid', Integer),
    Column('sentid', Integer)
)


def get_citation_context_records_with_offset(pmid: str):
    query_for_pubmed_records = Template(
        '''SELECT pmid, context_data,citations_list
            FROM citation_context
            WHERE pmid >= '$pmid'
            order by pmid
            LIMIT 3000;
        '''
    )
    start = time.perf_counter()
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query_for_pubmed_records.substitute(pmid=pmid))
            records = cursor.fetchall()
    end = time.perf_counter()
    logging.warning(f"Successfully fetched {len(records)} citation context records in {end - start} seconds")
    return records


def get_all_pmids():
    query = '''SELECT pmid
               FROM citation_context
               order by pmid;
               '''
    start = time.perf_counter()
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    end = time.perf_counter()
    logging.warning(f"Successfully fetched {len(rows)} rows in {end - start} seconds")
    return rows


def clean_raw_json_list(json_raw_list: list):
    filtered_list = []
    all_cited = []
    relevant_keys = ['pmid', 'paraid', 'sentid', 'sentence', 'citations']
    for tmp_dict in json_raw_list:
        if tmp_dict['has_citations'] and tmp_dict['citations']:
            curr_dict = {key: tmp_dict[key] for key in relevant_keys}
            filtered_list.append(curr_dict)
            for cited_id in curr_dict['citations']:
                all_cited.append(cited_id)
    return filtered_list, all_cited


def create_perfectly_relevant_list(filtered_list):
    # 1. Add perfectly relevant data points:
    perfectly_relevant_list = []
    paragraph_pairs = defaultdict(lambda: defaultdict(list))
    keys = ['pmid', 'paraid', 'sentid', 'sentence']
    for entry in filtered_list:
        for cited_id in entry['citations']:
            new_entry = {}
            for key in keys:
                new_entry[key] = entry[key]
            new_entry['cited_id'] = cited_id
            new_entry['relevance_score'] = 3
            perfectly_relevant_list.append(new_entry)
            paragraph_pairs[new_entry['paraid']][new_entry['sentid']].append(cited_id)

    return perfectly_relevant_list, paragraph_pairs


def create_somewhat_relevant_list(perfectly_relevant_list, paragraph_pairs):
    # 2. Add somewhat relevant data points which has score=2 :
    # Same paragraph different sentence id
    somewhat_relevant_list = []
    for entry in perfectly_relevant_list:
        paraid = entry.get('paraid')
        sentid = entry.get('sentid')

        paragraph_pairs_entries = deepcopy(paragraph_pairs.get(paraid))

        # Ignore cited IDs that are perfectly relevant when they appear in different sentences within the same paragraph
        cited_ids_to_ignore = set(paragraph_pairs_entries[sentid])

        paragraph_pairs_entries.pop(sentid)

        for k, v in paragraph_pairs_entries.items():
            for cited_id in v:
                if cited_id not in cited_ids_to_ignore:
                    new_entry = deepcopy(entry)
                    new_entry['cited_id'] = cited_id
                    new_entry['relevance_score'] = 2
                    somewhat_relevant_list.append(new_entry)
    return somewhat_relevant_list


# 3. Add slightly relevant data points :
# Different paragraph

def create_slightly_relevant_list(perfectly_relevant_list, paragraph_pairs):
    slightly_relevant_list = []
    for entry in perfectly_relevant_list:
        paraid = entry.get('paraid')
        paragraph_pairs_entries = deepcopy(paragraph_pairs)

        cited_ids_to_ignore = set()
        for ids in paragraph_pairs[paraid]:
            cited_ids_to_ignore.add(ids)

        paragraph_pairs_entries.pop(paraid)

        for k, v in paragraph_pairs_entries.items():
            for idx, ids in v.items():
                for cited_id in ids:
                    if cited_id not in cited_ids_to_ignore:
                        new_entry = deepcopy(entry)
                        new_entry['cited_id'] = cited_id
                        new_entry['relevance_score'] = 1
                        slightly_relevant_list.append(new_entry)

                        # Mark current cited id as visit to prevent redundancy within the same sentence
                        cited_ids_to_ignore.add(cited_id)
    return slightly_relevant_list


def create_irrelevant_list(perfectly_relevant_list, all_cited, all_pmids):
    # 4. Add irrelevant data points :
    irrelevant_list = []
    for record in perfectly_relevant_list:

        # Adding non-relevant row
        non_relevant = deepcopy(record)
        while True:
            pmid = random.choice(all_pmids)
            if pmid not in all_cited:
                break

        non_relevant['relevance_score'] = 0
        non_relevant['cited_id'] = pmid
        irrelevant_list.append(non_relevant)

    return irrelevant_list


def insert_clean_records(clean_records: list):
    start = time.perf_counter()
    insert_statement = insert(relevance_store)

    stmt = insert_statement.values(clean_records)

    # Execute the bulk upsert query
    try:
        with engine.begin() as connection:
            connection.execute(stmt)
    except Exception as e:
        logging.warning(f"Error in ingesting {e}", exc_info=True)

    end = time.perf_counter()
    logging.warning(f"Successfully inserted {len(clean_records)} relevance store records in {end - start} seconds")


if __name__ == '__main__':
    all_pmids = get_all_pmids()
    pmid_rows = [entry[0] for entry in all_pmids]
    pmid_rows.sort()
    offset = 0
    total_rows = len(pmid_rows)

    for idx in range(0, total_rows, 3000):
        final_list = []
        curr_pmid = pmid_rows[idx]
        records = get_citation_context_records_with_offset(pmid=curr_pmid)
        for record in records:
            context_data = json.loads(record['context_data'])
            citations_list = record.get('citations_list')
            if context_data and citations_list:
                filtered_list, all_cited = clean_raw_json_list(context_data)
                perfectly_relevant_list, paragraph_pairs = create_perfectly_relevant_list(filtered_list)
                somewhat_relevant_list = create_somewhat_relevant_list(perfectly_relevant_list, paragraph_pairs)
                slightly_relevant_list = create_slightly_relevant_list(perfectly_relevant_list, paragraph_pairs)
                irrelevant_list = create_irrelevant_list(perfectly_relevant_list, all_cited, all_pmids)
                final_list.extend(perfectly_relevant_list)
                final_list.extend(somewhat_relevant_list)
                final_list.extend(slightly_relevant_list)
                final_list.extend(irrelevant_list)

        insert_clean_records(final_list)
