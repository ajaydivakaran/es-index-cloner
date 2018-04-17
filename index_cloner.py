import argparse
import json
import pyes
import requests


class IndexCloner(object):
    def __init__(self, source_index, target_index, source_es_ip_port, target_es_ip_port, shard_count, replica_count):
        self.source_index = source_index
        self.target_index = target_index
        self.source_es_server = source_es_ip_port
        self.target_es_server = target_es_ip_port
        self.shard_count = shard_count
        self.replica_count = replica_count

    def clone(self):
        self._copy_mappings()
        self._copy_data()

    def merge(self, weaker, stronger):
        for key in stronger:
            if key in weaker:
                if isinstance(weaker[key], dict) and isinstance(stronger[key], dict):
                    self.merge(weaker[key], stronger[key])
                elif weaker[key] != stronger[key]:
                    weaker[key] = stronger[key]
            else:
                weaker[key] = stronger[key]
        return weaker

    def _copy_mappings(self):
        conn = pyes.ES(self.source_es_server)
        base_index_settings = {
            "settings": {"index": {"number_of_shards": self.shard_count, "number_of_replicas": self.replica_count}}
        }
        source = self._get_index()
        index_settings = self.merge(source, base_index_settings)
        conn.indices.create_index_if_missing(self.target_index, index_settings)

    def _get_index(self):
        r = requests.get('%s/%s' % (self.source_es_server, self.source_index))
        assert r.status_code == 200, "Failed to retrieve index: %s" % self.source_index
        source = json.loads(r.content)
        return source.values().pop()

    def _get_mappings(self):
        r = requests.get('%s/%s/_mapping' % (self.source_es_server, self.source_index))
        assert r.status_code == 200, "Failed to retrieve mappings from index: %s" % self.source_index
        source_mappings = json.loads(r.content)
        return source_mappings.values().pop()

    def _copy_data(self):
        source_conn = pyes.ES(self.source_es_server)
        target_conn = pyes.ES(self.target_es_server)
        search = pyes.query.MatchAllQuery().search(bulk_read=1000)
        mappings_types = self._get_mappings()["mappings"].keys()
        for type in mappings_types:
            hits = source_conn.search(search, self.source_index, type, scan=True, scroll="30m", model=lambda _, hit: hit)
            for hit in hits:
                target_conn.index(hit['_source'], self.target_index, type, hit['_id'], bulk=True)
            target_conn.indices.flush(self.target_index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clone elasticsearch index')
    parser.add_argument('-s', action="store", dest='source_index', help="Source index to copy from")
    parser.add_argument('-t', action="store", dest='target_index', help="Target index")
    parser.add_argument('-e', action="store", dest='source_es_server', default="http://127.0.0.1:9200", help="Elasticsearch ip:port - default(http://127.0.0.1:9200)")
    parser.add_argument('-d', action="store", dest='target_es_server', default="http://127.0.0.1:9200", help="Elasticsearch ip:port - default(http://127.0.0.1:9200)")
    parser.add_argument('-p', action="store", dest='primary_shards', default=3, help="primary shards in target index - default(3)")
    parser.add_argument('-r', action="store", dest='replica_shards', default=0, help="replica shards in target index - default(0)")
    arguments = parser.parse_args()

    IndexCloner(arguments.source_index, arguments.target_index, arguments.source_es_server, arguments.target_es_server,
                arguments.primary_shards, arguments.replica_shards).clone()
