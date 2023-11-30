"""
1) Fetch data in batches from citation_context table
2) Split into multi processes to clean the relevance data
3) Pair each record with a non-relevant row before inserting
"""
import time
from string import Template
import logging
import random
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from copy import deepcopy
from sqlalchemy import create_engine, MetaData, Table, Column, String, Boolean, insert

logging.basicConfig(filename='logs/ingest_relevance_store.log', filemode='a',
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
    'relevance_store', metadata,
    Column('pmid', String),
    Column('sentence', String),
    Column('cited', String),
    Column('is_relevant', Boolean)
)


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


def get_citation_context_records_with_offset(pmid: str):
    query_for_pubmed_records = Template(
        '''SELECT pmid, context_data
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
    logging.warning(f"Successfully fetched {len(records)} citatation context records in {end - start} seconds")
    return records


def clean_pubmed_records(pre_clean_records: list):
    relevant_rows = []
    for entry in pre_clean_records:
        pmid = entry.get('pmid')
        data = entry.get('context_data')
        data = json.loads(data)
        if not data:
            continue
        for record in data:
            has_citations = record.get('has_citations')
            citations = record.get('citations')
            if has_citations and citations:
                pmid = record.get('pmid')
                sentence = record.get('sentence')
                for citation in citations:
                    entry = {'pmid': pmid, 'sentence': sentence, 'cited': citation, 'is_relevant': 1}
                    relevant_rows.append(entry)
        return relevant_rows


def add_non_relevant_records(cleaned_records: list, all_pmids: list):
    final_records = []

    for record in cleaned_records:
        final_records.append(record)

        # Adding non-relevant row
        non_relevant = deepcopy(record)
        while True:
            pmid = random.choice(all_pmids)
            if pmid != non_relevant['pmid']:
                break

        non_relevant['is_relevant'] = 0
        non_relevant['cited'] = pmid
        final_records.append(non_relevant)

    return final_records


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
    pmid_rows = get_all_pmids()
    pmid_rows = [entry[0] for entry in pmid_rows]

    offset = 0
    total_rows = len(pmid_rows)
    # total_rows = 3000

    for idx in range(0, total_rows, 3000):
        curr_pmid = pmid_rows[idx]
        records = get_citation_context_records_with_offset(pmid=curr_pmid)
        relevant_rows = clean_pubmed_records(pre_clean_records=records)
        final_rows = add_non_relevant_records(cleaned_records=relevant_rows, all_pmids=pmid_rows)
        if final_rows:
            insert_clean_records(final_rows)
