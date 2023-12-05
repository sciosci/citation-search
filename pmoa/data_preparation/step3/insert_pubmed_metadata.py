import logging
import time
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Boolean, insert, Integer

logging.basicConfig(filename='logs/ingest_pubmed_metadata.log', filemode='a',
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

metadata_store = Table(
    'metadata', metadata,
    Column('pmid', String, primary_key=True),
    Column('title', String),
    # Column('abstract', String),
    Column('publication_year', Integer),
    Column('cited_by_count', Integer)
)


def read_metadata_parquet_file(file_name: str):
    df = pd.read_parquet(file_name)
    df.drop('doi', axis=1, inplace=True)
    new_names = {'PubMedID': 'pmid'}
    df = df.rename(columns=new_names)
    df['pmid'] = df['pmid'].astype(str)
    dict_records = df.to_dict('records')
    return dict_records


def insert_metadata_records(records: list):
    start = time.perf_counter()
    insert_statement = insert(metadata_store)
    stmt = insert_statement.values(records)

    # Execute the bulk upsert query
    try:
        with engine.begin() as connection:
            connection.execute(stmt)
    except Exception as e:
        logging.warning(f"Error in ingesting {e}", exc_info=True)

    end = time.perf_counter()
    logging.warning(f"Successfully inserted {len(records)} relevance store records in {end - start} seconds")


if __name__ == '__main__':
    source_file_name = 'pubmed_metadata_20231204.parquet'
    records = read_metadata_parquet_file(file_name=source_file_name)
    insert_metadata_records(records=records)
