import time
from recordsearch_tools.client import RSSearchClient, RSItemClient
from pymongo import MongoClient
from credentials import MONGOLAB_URL
import random

client = RSSearchClient()
client.search(access='Closed')


class SearchHarvester():
    """
    Harvest the details of 'Closed' files from RecordSearch.
    Saves to MongoDB.
    harvester = SearchHarvester()
    harvester.start_harvest()
    """
    def __init__(self, **kwargs):
        self.total_pages = None
        self.pages_complete = 0
        self.client = RSSearchClient()
        self.prepare_harvest(access='Closed')
        db = self.get_db()
        self.items = db.items

    def get_db(self):
        dbclient = MongoClient(MONGOLAB_URL)
        db = dbclient.get_default_database()
        return db

    def get_total(self):
        return self.client.total_results

    def prepare_harvest(self, **kwargs):
        self.client.search(**kwargs)
        total_results = self.client.total_results
        print '{} items'.format(total_results)
        self.total_pages = (int(total_results) / self.client.results_per_page) + 1
        print '{} pages'.format(self.total_pages)

    def start_harvest(self, page=None):
        item_client = RSItemClient()
        if not page:
            page = self.pages_complete + 1
        else:
            self.pages_complete = page - 1
        while self.pages_complete < self.total_pages:
            response = self.client.search(access='Closed', page=page, sort='9')
            for result in response['results']:
                item = item_client.get_summary(entity_id=result['identifier'])
                item['_id'] = item['identifier']
                item['random_id'] = [random.random(), 0]
                self.items.update({'_id': item['identifier']}, item, upsert=True)
                print item['identifier']
            self.pages_complete += 1
            page += 1
            print '{} pages complete'.format(self.pages_complete)
            time.sleep(1)
