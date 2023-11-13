from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
import json
import os

username = 'admin'
password = 'admin'
host = '127.0.0.1'
port = '5432'
database = 'citation_aggregated_model'

connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(connection_string)

metadata = MetaData()

citation_abstract = Table(
    'citation_abstract', metadata,
    Column('corpus_id', Integer, primary_key=True),
    Column('abstract', String)
)

citation_papers_meta = Table(
    'citation_papers_meta', metadata,
    Column('corpus_id', Integer, primary_key=True),
    Column('title', String),
    Column('venue', String),
    Column('year', Integer),
    Column('reference_count', Integer),
    Column('citation_count', Integer)
)


# Generate the bulk upsert query
def bulk_upload_papers(data: list):
    insert_statement = insert(citation_abstract)
    stmt = insert_statement.values(data).on_conflict_do_update(
        index_elements=['corpus_id'],
        set_={
            'abstract': insert_statement.excluded.abstract
        }
    )

    # Execute the bulk upsert query
    with engine.begin() as connection:
        connection.execute(stmt)


release_id = '2023-05-16'
dataset = 'abstracts'  # ["s2orc", "papers", "abstracts", "authors", "citations"]
source_path = f"/home/ubuntu/mypetalibrary/semantic-scholar/{release_id}/{dataset}/extracted"
files = os.listdir(source_path)
for file in files:
    inp_file = f"{source_path}/{file}"
    with open(inp_file, "r") as f:
        line = f.readline()
        abstracts = []
        corpus_id_tracker = set()
        while line:
            line_mod = json.loads(line)
            corpus_id = line_mod.get("corpusid")
            abstract = line_mod.get("abstract")

            if corpus_id not in corpus_id_tracker:
                abstracts.append({"corpus_id": str(corpus_id),
                                  "abstract": abstract
                                  })
                corpus_id_tracker.add(corpus_id)
            if len(abstracts) == 2500:
                bulk_upload_papers(abstracts)
                abstracts = []
                corpus_id_tracker = set()
            line = f.readline()

        if abstracts:
            bulk_upload_papers(abstracts)
