from string import Template
import psycopg2
import time
import pandas as pd

conn_params = {
    "host": "10.224.68.29",
    "port": "5432",
    "database": "citation_aggregated_model",
    "user": "admin",
    "password": "admin"
}

query_for_title_abstract = Template('''SELECT cat.corpus_id
        FROM citation_abstract cat
                 JOIN citation_papers_meta cpm on cat.corpus_id = cpm.corpus_id
        WHERE cat.corpus_id>= $cid
        group by
            cat.corpus_id
        LIMIT $limit;
''')

start_time = time.perf_counter()
start = 0
last_cid = 0
end = 96873957
df = pd.DataFrame({'corpus_id': []})
for i in range(start, end, 1000000):
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query_for_title_abstract.substitute(cid=last_cid, limit=1000000))
            rows = cursor.fetchall()
            rows = list(rows)
            rows = [str(i[0]) for i in rows]
            last_cid = str(rows[-1])
            new_df = pd.DataFrame(rows, columns=['corpus_id'])
            df = df._append(new_df, ignore_index=True)
df.to_csv('corpus_ids.csv')
end_time = time.perf_counter()

print(f"Successfully filtered {end} rows in time {end_time-start_time} seconds")