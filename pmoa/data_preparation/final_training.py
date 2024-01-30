from sentence_transformers import SentenceTransformer, util
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, insert
import time
import psycopg2
import numpy as np
from psycopg2.extras import DictCursor
from catboost import CatBoostRanker, Pool, MetricVisualizer
from collections import defaultdict


def cosine_similarity(feature1, feature2):
    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    feature1_embedding = model.encode(feature1)
    feature2_embedding = model.encode(feature2)
    cosine_similarity = util.cos_sim(feature1_embedding, feature2_embedding)
    return cosine_similarity.item()


def get_some_records():
    conn_params = {
        "host": "10.224.68.29",
        "port": "5432",
        "database": "pubmed",
        "user": "admin",
        "password": "admin"
    }
    engine = create_engine('postgresql://', connect_args=conn_params)

    query = '''
            SELECT rs.pmid as pmid, rs.sentence as q_sentence, rs.cited_id as cited_id, rs.relevance_score as relevance_score,
                   m.title as q_title,cited.title as c_title,m.publication_year as q_year,cited.publication_year as c_year,
                   cited.cited_by_count as c_in_citations
            FROM relevance_store_new as rs
            LEFT JOIN metadata as m ON m.pmid = rs.pmid
            LEFT JOIN metadata as cited ON cited.pmid = rs.cited_id
            WHERE rs.pmid IN (select DISTINCT pmid from metadata)
              AND rs.cited_id IN (select DISTINCT pmid from metadata);
            '''
    start = time.perf_counter()
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
    end = time.perf_counter()
    print(f"Time taken to fetch records with metadata: {end - start} seconds")

    grouped_records = defaultdict(list)
    for record in records:
        grouped_records[record['pmid']].append(record)
    print(f"Total groups: {len(grouped_records)}")

    return grouped_records


def get_embedded_records(grouped_records):
    embedded_groups = defaultdict(list)
    cosine_pairs = [
        ('q_title', 'c_title'),
        ('q_sentence', 'c_title')
    ]
    '''
    year_difference = q_year-c_year
    len_c_title = len(c_title)
    len_c_abstract = len(c_abstract)
    log_c_in_citations = np.log2(c_in_citations)
    '''
    start = time.perf_counter()
    for k, v in grouped_records.items():
        for record in v:
            embedding_arr = []
            for a, b in cosine_pairs:
                if not record.get(a):
                    record[a] = ''
                if not record.get(b):
                    record[b] = ''
                embedding_arr.append(cosine_similarity(record.get(a, ''), record.get(b, '')))
            embedding_arr.append(record.get('q_year') - record.get('c_year'))
            embedding_arr.append(len(record.get('c_title', '')))
            if record.get('c_in_citations'):
                embedding_arr.append(np.log2(record.get('c_in_citations')))
            else:
                embedding_arr.append(0)
            embedding_arr.append(record.get('relevance_score'))
            embedded_groups[k].append(embedding_arr)
    end = time.perf_counter()
    print(f"Time taken to create embeddings for {len(embedded_groups)} groups: {end - start} seconds")
    return embedded_groups


def get_train_and_test_pools(embedded_groups):
    test_size = 0.2
    test_groups_size = int(len(embedded_groups) * test_size)
    train_groups_size = len(embedded_groups) - test_groups_size
    test_groups = list(embedded_groups.keys())[:test_groups_size]
    train_groups = list(embedded_groups.keys())[test_groups_size:]

    test_data = []
    test_queries = []
    for test_group in test_groups:
        for group in embedded_groups[test_group]:
            test_data.append(group)
            test_queries.append(test_group)
    X_test = [data[:-1] for data in test_data]
    y_test = [data[-1] for data in test_data]

    train_data = []
    train_queries = []
    for train_group in train_groups:
        for group in embedded_groups[train_group]:
            train_data.append(group)
            train_queries.append(train_group)
    X_train = [data[:-1] for data in train_data]
    y_train = [data[-1] for data in train_data]

    max_relevance = max(np.max(y_train), np.max(y_test))
    y_train /= max_relevance
    y_test /= max_relevance

    train = Pool(
        data=X_train,
        label=y_train,
        group_id=train_queries
    )

    test = Pool(
        data=X_test,
        label=y_test,
        group_id=test_queries
    )

    return train, test


if __name__ == '__main__':
    grouped_records = get_some_records()
    embedded_groups = get_embedded_records(grouped_records)
    train_pool, test_pool = get_train_and_test_pools(embedded_groups)
