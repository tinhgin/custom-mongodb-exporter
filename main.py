import os
import sys
import time
import logging
from pymongo import MongoClient
from prometheus_client import start_http_server, Gauge

logger = logging.getLogger('CUSTOM_MONGODB_EXPORTER')
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

g = Gauge('mongodb_shard_distribution',
          'shard_data, shard_docs, shard_chunks, estimated_data_per_chunk, estimated_docs_per_chunk',
          ["database", "collection", "shard_name", "shard_host", "data"])

database_uri = os.environ['MONGODB_URI']
database_name = os.environ['DATABASE_NAME']
collection_name_list = os.environ['COLLECTIONS'].replace(' ', '').split(',')

client = MongoClient(database_uri)
db = client[database_name]
db_config = client['config']
collection_config = db_config['collections']


def get_shard_distribution(collection):
    shard_distribution = []
    collection_stats = db.command("collStats", collection)

    if not collection_stats['sharded']:
        logger.debug("Collection " + collection + " is not sharded.")
        return None

    cursor = collection_config.find({'_id': database_name + "." + collection})
    collection_uuid = cursor[0]['uuid']
    cursor.close()

    for shard in collection_stats['shards']:
        collection = db_config['shards']
        cursor = collection.find({'_id': shard})
        shard_host = cursor[0]['host']
        cursor.close()

        shard_data = collection_stats['shards'][shard]['size']

        shard_docs = collection_stats['shards'][shard]['count']

        collection = db_config['chunks']
        cursor = collection.find({'uuid': collection_uuid, 'shard': shard})
        shard_chunks = len(list(cursor))
        cursor.close()

        estimated_data_per_chunk = int(shard_data / shard_chunks)

        estimated_docs_per_chunk = int(shard_docs / shard_chunks)

        shard_distribution.append({'shard_name': shard, 'shard_host': shard_host, 'shard_data': shard_data,
                                   'shard_docs': shard_docs, 'shard_chunks': shard_chunks,
                                   'estimated_data_per_chunk': estimated_data_per_chunk,
                                   'estimated_docs_per_chunk': estimated_docs_per_chunk})

    return shard_distribution


if __name__ == '__main__':
    try:
        sleep_time = int(os.environ['INTERVAL'])
        logger.info("Environment variable INTERVAL=%s" % sleep_time)
    except KeyError:
        sleep_time = 1800
        logger.debug("Environment variable INTERVAL is not set. Use default INTERVAL=%s" % sleep_time)

    # Start up the server to expose the metrics.
    start_http_server(8890)
    # Generate some requests.
    while True:
        try:
            for collection_name in collection_name_list:
                raw_metrics = get_shard_distribution(collection_name)
                for raw_metric in raw_metrics:
                    g.labels(database=database_name, collection=collection_name, shard_name=raw_metric['shard_name'],
                             shard_host=raw_metric['shard_host'], data='shard_data').set(raw_metric['shard_data'])
                    g.labels(database=database_name, collection=collection_name, shard_name=raw_metric['shard_name'],
                             shard_host=raw_metric['shard_host'], data='shard_docs').set(raw_metric['shard_docs'])
                    g.labels(database=database_name, collection=collection_name, shard_name=raw_metric['shard_name'],
                             shard_host=raw_metric['shard_host'], data='shard_chunks').set(raw_metric['shard_chunks'])
                    g.labels(database=database_name, collection=collection_name, shard_name=raw_metric['shard_name'],
                             shard_host=raw_metric['shard_host'],
                             data='estimated_data_per_chunk').set(raw_metric['estimated_data_per_chunk'])
                    g.labels(database=database_name, collection=collection_name, shard_name=raw_metric['shard_name'],
                             shard_host=raw_metric['shard_host'],
                             data='estimated_docs_per_chunk').set(raw_metric['estimated_docs_per_chunk'])
        except Exception as e:
            logger.debug(e)
        time.sleep(sleep_time)
