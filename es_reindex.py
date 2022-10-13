from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import OrderedDict
import traceback
import datetime
import json

f = open('config.json')
CONFIG = json.loads(f.read())
f.close()

print(CONFIG)

def elastic_config(host, port):
    """
    Method to connect elastic search
    """

    es = Elasticsearch([{'host': host, 'port': port}], timeout=60, max_retries=30, retry_on_timeout=True)
    return es

def dump_review_to_elastic(es, _index, _doc_type, _id, _body):

    """
    Method to dump a review into elastic search with specific index,id and doc_type
    """

    try:
        # create index
        es.index(index=_index, doc_type=_doc_type, id=_id, body=_body)
    except:
        traceback.print_exc()
        pass
ldate = (datetime.datetime.now()+datetime.timedelta(-150)).isoformat().split("T")[0]
udate = datetime.datetime.now().isoformat().split("T")[0]
final_query = "created_at:[%s TO %s]"%(ldate, udate)

elastic_obj = elastic_config(CONFIG['SOURCE'].split(":")[0], int(CONFIG['SOURCE'].split(":")[1]))
elastic_obj_new = elastic_config(CONFIG['DEST'].split(":")[0], int(CONFIG['DEST'].split(":")[1]))
page = helpers.scan(elastic_obj,
           query={
                  "query": {
                   "query_string": {
                     "query": final_query
                   }
                 }
                },
           index=CONFIG["SOURCE_INDEX"]
    )

count = 1

actions = []
for p in page:
    try:
        count += 1
        if count > 0:
            # p['_source'].pop('credibility', None)
            try:
                action = {
                    "_index": CONFIG["DEST_INDEX"],
                    "_id": p['_id'], "_source": p['_source']}
                actions.append(action)

                if count % 5000 == 0:
                    helpers.bulk(elastic_obj_new, actions)
                    actions = []
                    print(count)

            except:
                traceback.print_exc()
                input(">>>")
    except:
        traceback.print_exc()
        input(">>")

helpers.bulk(elastic_obj_new, actions)