import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import RealDictCursor

conn_params = {
    "host": "10.224.68.29",
    "port": "5432",
    "database": "pubmed",
    "user": "admin",
    "password": "admin"
}

engine = create_engine('postgresql://', connect_args=conn_params)


def get_citation_context_records_with_offset():
    query_for_pubmed_records = 'SELECT * FROM relevance_store;'
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query_for_pubmed_records)
            records = cursor.fetchall()
    return records


records = get_citation_context_records_with_offset()

unique_ids = set()

for record in records:
    unique_ids.add(record.get('pmid'))
    unique_ids.add(record.get('cited'))

unique_ids = list(unique_ids)

df = pd.DataFrame(unique_ids, columns=['PubMedID'])

df.to_csv('pub_med_ids.csv')
