# gunicorn -b localhost:6001 app:app
import logging

import pandas as pd
from flask import Flask, request, Response
import json

app = Flask(__name__)
logging.basicConfig(filename='logs/get_corpus_ids.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')
df = pd.read_csv('corpus_ids.csv')
corpus_ids = df['corpus_id'].values


@app.route('/corpus_ids')
def get_corpus_ids():
    index = int(request.args.get("idx"))
    # Dataframe logic
    cid = str(corpus_ids[index])
    response = {"cid": cid}
    logging.warning(f"Sent corpus id : {cid} for index : {index}")
    return Response(json.dumps(response),
                    status=200,
                    mimetype='application/json')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=6001)
