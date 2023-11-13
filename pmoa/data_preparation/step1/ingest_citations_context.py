import multiprocessing

from sqlalchemy import create_engine, Column, String, MetaData, Table, JSON, ARRAY
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import logging
import json

from article_has_citation import get_etree_with_path, Article

logging.basicConfig(filename='logs/mpoa_xml_extract.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')

# Basic Setup for database
username = 'admin'
password = 'admin'
host = '127.0.0.1'
port = '5432'
database = 'pubmed'

connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(connection_string)

metadata = MetaData()

citation_context = Table(
    'citation_context', metadata,
    Column('pmid', String, primary_key=True),
    Column('context_data', JSON),
    Column('citations_list', ARRAY(String))

)


def extract_citation_context(path: str):
    print(f"Started processing {path}")
    result = get_etree_with_path(path=path)
    article = Article(root=result)
    meta = article.build_meta_list_all_level()
    pmid = meta[0][0]['pmid']
    citations = list(meta[0][0]['citations'])

    # Clean context information to make it serializable
    context = meta[2]
    for entry in context:
        entry['citations'] = list(entry['citations'])
    context = json.dumps(context)

    result_dict = {'pmid': pmid,
                   'citations_list': citations,
                   'context_data': context}
    logging.warning(f"Finished processing {path}")
    return result_dict


def bulk_upload_citation_context_entries(data: list):
    insert_statement = insert(citation_context)
    stmt = insert_statement.values(data).on_conflict_do_update(
        index_elements=['pmid'],
        set_={
            'context_data': insert_statement.excluded.context_data,
            'citations_list': insert_statement.excluded.citations_list
        }
    )

    # Execute the bulk upsert query
    try:
        with engine.begin() as connection:
            connection.execute(stmt)
    except Exception as e:
        logging.warning(f"Error in ingesting {e}", exc_info=True)


# Start bulk inserting into database

start = 141813
end = 3061700
path = '/home/ubuntu/mypetalibrary/pmoa-cite-dataset'
df_path = f'{path}/oa_comm_files.csv'
df = pd.read_csv(df_path)
pm_ids = df['0'].values

curr_idx = start
peta_lib_path = '/home/ubuntu/mypetalibrary/pmc-oa-opendata/oa_comm'

cores = 10

while curr_idx < end:
    paths = []
    for idx in range(curr_idx, curr_idx + 200):
        if idx < end:
            file = pm_ids[idx]
            final_path = f'{peta_lib_path}/{file}'
            paths.append(final_path)
    with multiprocessing.Pool(processes=cores) as pool:
        entries = pool.map(extract_citation_context, paths)

    # Fix entries removing duplicate pmid
    pmid_set = set()
    final_entries = []
    for entry in entries:
        pmid = entry.get('pmid')
        if pmid and pmid not in pmid_set:
            final_entries.append(entry)
            pmid_set.add(pmid)
    curr_idx += 200
    bulk_upload_citation_context_entries(data=entries)
    logging.warning(f"Successfully inserted {curr_idx} entries")
