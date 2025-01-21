from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from logging import ERROR, WARNING
from requests_cache import CachedSession
import requests
import json




class CachingOgcFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(CachingOgcFdw, self).__init__(options, columns)
        self.collection_id = options.get('collection_id', None)
        if self.collection_id is None:
            log_to_postgres('You MUST set the collection ID',ERROR)
        self.session = CachedSession('demo_cache', backend='sqlite', use_temp=True)
        self.columns = columns

    def run(self):
        r = self.session.get("https://ogc-compliance.iudx.io/collections/"+self.collection_id+"/items")
        resp = r.json()['features']
        for i in resp:
            yield {
                    **i['properties'],
                    "id": i["id"],
                    "geom":json.dumps(i["geometry"]),
            }

    def execute(self, quals, columns):
        return self.run()

