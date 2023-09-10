import psycopg2
import time
from psycopg2.extras import RealDictCursor
from langdetect import DetectorFactory, detect
from string import Template
from time import sleep
import json
import requests
from requests.auth import HTTPBasicAuth
import logging
import multiprocessing
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

logging.basicConfig(filename='logs/ingest_vector.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')

conn_params = {
    "host": "10.224.68.29",
    "port": "5432",
    "database": "citation_aggregated_model",
    "user": "admin",
    "password": "admin"
}

query_for_title_abstract = Template(
    '''SELECT cat.corpus_id,
                   cat.abstract,
                   cpm.title
            FROM citation_abstract cat
                     JOIN citation_papers_meta cpm on cat.corpus_id = cpm.corpus_id
            group by
                cat.corpus_id,
                cpm.title
            LIMIT 3000
            OFFSET $offset ;
    ''')


def deploy_model():
    os_url = 'http://localhost:9200'
    model_id = "Rld_cIoBXeh4DoQaprB5"
    query = f"{os_url}/_plugins/_ml/models/{model_id}/_load"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(query, auth=HTTPBasicAuth('admin', 'admin'), headers=headers)
    sleep(10)


def bulk_upload_using_client(records: list):
    auth = ('admin', 'admin')
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        http_compress=True,
        http_auth=auth
    )
    index_name = 'citation_store_with_title_and_abstract'
    bulk_data = []
    for doc in records:
        doc["_id"] = doc.get('corpus_id')
        doc["_index"] = index_name
        bulk_data.append(doc)

    success, failed = bulk(client, bulk_data)
    logging.warning(f"Success: {success}; Failed: {failed}")


def bulk_upload_records(records: list):
    logging.warning(f"Start inserting {len(records)} records in opensearch")
    os_url = 'http://localhost:9200/'
    index_name = 'citation_store_with_title_and_abstract'

    # Define the Elasticsearch bulk insert payload
    bulk_data = ''
    for doc in records:
        idx = doc.get('corpus_id')
        bulk_data += f'{{"index":{{"_index":"{index_name}","_id":"{idx}"}}}}\n'
        bulk_data += f'{json.dumps(doc)}\n'

    headers = {'Content-Type': 'application/json'}

    # Insert the data using the OpenSearch bulk API
    response = requests.post(f'{os_url}_bulk', auth=HTTPBasicAuth('admin', 'admin'),
                             headers=headers, data=bulk_data)
    logging.warning(f'Successfully inserted {len(records)} documents into "{index_name}"')
    # Check the response status code
    if response.status_code != 200:
        logging.warning(f'Error inserting data: {response.content}')
    logging.warning(f"Completed inserting {len(records)} records in opensearch")


def get_records_from_db_into_one_entry(offset: int):
    start = time.perf_counter()
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query_for_title_abstract.substitute(offset=offset))
            rows = cursor.fetchall()
    end = time.perf_counter()
    logging.warning(f"Successfully fetched {len(rows)} rows in {end - start} seconds")

    start = time.perf_counter()
    DetectorFactory.seed = 0
    cleaned_rows = []
    for row in rows:
        title = row.get('title')
        try:
            if title and detect(title) == 'en':
                new_row = dict()
                new_row['corpus_id'] = str(row['corpus_id'])
                new_row['concat'] = row.get('title') + '[SEP]' + row.get('abstract')
                cleaned_rows.append(new_row)
        except Exception as exc:
            logging.warning(f"Error in title :{title}: {exc}", exc_info=True)
    end = time.perf_counter()
    logging.warning(f"Successfully filtered {len(cleaned_rows)} in {end - start} seconds")
    return cleaned_rows


if __name__ == "__main__":

    deploy_model()
    # deploy_model_using_client()
    start = 0
    total = 1000000
    # total = 96873957
    offsets = list()
    total_entries = 0
    start_time = time.perf_counter()
    for i in range(start, total + 1, 3000):
        offsets.append(i)
        if len(offsets) == 20 or i + 3000 > total:
            # Fetch cleaned rows
            with multiprocessing.Pool(processes=22) as pool:
                cleaned_rows = pool.map(get_records_from_db_into_one_entry, offsets)
            # Insert cleaned rows
            with multiprocessing.Pool(processes=22) as pool:
                # pool.map(bulk_upload_using_client, cleaned_rows)
                pool.map(bulk_upload_records, cleaned_rows)
            sleep(10)

            curr_entries = sum(len(inner_list) for inner_list in cleaned_rows)
            total_entries += curr_entries
            logging.warning(
                f"Successfully inserted {len(offsets)} offsets chunks with {curr_entries} "
                f"records, with current last offset {i}")
            offsets = []
            cleaned_rows = []
            sleep(10)
    end_time = time.perf_counter()
    logging.warning(
        f"Total time taken from start to end is {end_time - start_time} seconds to insert {total_entries} records")
