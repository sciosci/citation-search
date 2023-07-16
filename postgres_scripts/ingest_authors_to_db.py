import os
import json
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, BigInteger
from sqlalchemy.dialects.postgresql import insert

username = 'admin'
password = 'admin'
host = '127.0.0.1'
port = '5432'
database = 'citation_aggregated_model'

connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(connection_string)
metadata = MetaData()

citation_authors = Table(
    'citation_author', metadata,
    Column('corpus_id', Integer, primary_key=True),
    Column('author_id', BigInteger),
    Column('name', String,primary_key=True)

)


def bulk_upload_authors(data: list):
    insert_statement = insert(citation_authors)
    stmt = insert_statement.values(data).on_conflict_do_update(
        index_elements=['corpus_id', 'name'],
        set_={
            'author_id': insert_statement.excluded.author_id
        }
    )

    # Execute the bulk upsert query
    with engine.begin() as connection:
        connection.execute(stmt)


release_id = '2023-05-16'
dataset = 'papers'  # ["s2orc", "papers", "abstracts", "authors", "citations"]
source_path = f"/home/ubuntu/mypetalibrary/semantic-scholar/{release_id}/{dataset}/extracted"
files = os.listdir(source_path)
for file in files:
    inp_file = f"{source_path}/{file}"
    with open(inp_file, "r") as f:
        line = f.readline()
        authors = []
        corpus_id_tracker = set()
        while line:
            line_mod = json.loads(line)
            corpus_id = line_mod.get("corpusid")
            authors_line = line_mod.get("authors")

            for entry in authors_line:
                author_id = entry.get("authorId", "")
                author_name = entry.get("name", "")
                corpus_comb_id = f"{corpus_id}:{author_name}"
                if author_name and corpus_comb_id not in corpus_id_tracker:
                    corpus_id_tracker.add(corpus_comb_id)
                    authors.append({"corpus_id": str(corpus_id),
                                    "author_id": author_id,
                                    "name": author_name
                                    })
            if len(authors) >= 2500:
                bulk_upload_authors(authors)
                authors = []
                corpus_id_tracker = set()
            line = f.readline()

        if authors:
            bulk_upload_authors(authors)
