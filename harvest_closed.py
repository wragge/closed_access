import time
from recordsearch_tools.client import RSSearchClient, RSItemClient, RSSeriesClient, UsageError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from credentials import MONGOLAB_URL
import random
import re
from datetime import datetime


EXCEPTIONS = [
    ['33(1)(a)', r'33\(1\)\(a\)'],
    ['33(1)(b)', r'33\(1\)[a\(\)]*\(b\)'],
    ['33(1)(c)', r'33\(1\)[ab\(\)]*\(c\)'],
    ['33(1)(d)', r'33\(1\)[abc\(\)]*\(d\)'],
    ['33(1)(e)(i)', r'33\(1\)[abcd\(\)]*\(e\)\(i\)'],
    ['33(1)(e)(ii)', r'33\(1\)[abcd\(\)]*\(e\)\(ii\)'],
    ['33(1)(e)(iii)', r'33\(1\)[abcd\(\)]*\(e\)\(iii\)'],
    ['33(1)(f)(i)', r'33\(1\)[abcdei\(\)]*\(f\)\(i\)'],
    ['33(1)(f)(ii)', r'33\(1\)[abcdei\(\)]*\(f\)\(ii\)'],
    ['33(1)(f)(iii)', r'33\(1\)[abcdei\(\)]*\(f\)\(iii\)'],
    ['33(1)(g)', r'33\(1\)[abcdefi\(\)]*\(g\)*'],
    ['33(1)(h)', r'33\(1\)[abcdefgi\(\)]*\(h\)'],
    ['33(1)(j)', r'33\(1\)[abcdefghi\(\)]*\(j\)'],
    ['33(2)(a)', r'33\(2\)\(a\)'],
    ['33(2)(b)', r'33\(2\)[a\(\)]*\(b\)'],
    ['33(3)(a)(i)', r'33\(3\)\(a\)\(i\)'],
    ['33(3)(a)(ii)', r'33\(3\)\(a\)\(ii\)'],
    ['33(3)(b)', r'33\(3\)[ai\(\) &]*\(b\)'],
    ['Closed period', r'Closed period.*']
]


class SearchHarvester():
    """
    Harvest the details of 'Closed' files from RecordSearch.
    Saves to MongoDB.
    harvester = SearchHarvester(harvest='2015-12-31')
    harvester.start_harvest()
    """
    def __init__(self, harvest, **kwargs):
        self.total_pages = None
        self.pages_complete = 0
        self.client = RSSearchClient()
        self.prepare_harvest(access='Closed')
        db = self.get_db()
        self.items = db.items
        self.series = db.series
        self.harvests = db.harvests
        self.set_harvest(harvest)

    def set_harvest(self, harvest):
        # self.harvests.create_index('harvest_date', unique=True)
        dates = harvest.split('-')
        harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
        try:
            self.harvests.insert({'harvest_date': harvest_date})
        except DuplicateKeyError:
            pass
        self.harvest_date = harvest_date

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
        series_client = RSSeriesClient()
        # Refresh series with each harvest
        # self.series.remove({})
        # self.items.remove({})
        if not page:
            page = self.pages_complete + 1
        else:
            self.pages_complete = page - 1
        while self.pages_complete < self.total_pages:
            response = self.client.search(access='Closed', page=page, sort='9')
            for result in response['results']:
                exists = self.items.find_one({'_id': result['identifier'], 'harvests': self.harvest_date})
                if not exists:
                    item = item_client.get_summary(entity_id=result['identifier'])
                    item['_id'] = item['identifier']
                    item['random_id'] = [random.random(), 0]
                    # Normalise reasons
                    item['reasons'] = []
                    # item['year'] = item['contents_dates']['end_date']['date'].year
                    for reason in item['access_reason']:
                        matched = False
                        for exception, pattern in EXCEPTIONS:
                            if re.match(pattern, reason['reason']):
                                item['reasons'].append(exception)
                                matched = True
                        if not matched:
                            item['reasons'].append(reason['reason'])
                    # Get series and agency info
                    print item['series']
                    series = self.series.find_one({'identifier': item['series']})
                    if not series:
                        try:
                            series = series_client.get_summary(entity_id=item['series'], include_access_status=False)
                            # agencies = series_client.get_controlling_agencies(entity_id=item['series'])
                            # series['controlling_agencies'] = agencies
                            self.series.insert(series)
                        except UsageError:
                            series = None
                    if series:
                        item['series_title'] = series['title']
                        if series['controlling_agencies']:
                            item['agencies'] = []
                            for agency in series['controlling_agencies']:
                                if not agency['end_date'] or not agency['end_date']['date']:
                                    item['agencies'].append(agency)
                    item['harvests'] = [self.harvest_date]
                    self.items.insert_one(item)
                    print item['identifier']
            self.pages_complete += 1
            page += 1
            print '{} pages complete'.format(self.pages_complete)
            time.sleep(1)


def aggregate_reasons(harvest):
    '''Save aggregations for better performance'''
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    dates = harvest.split('-')
    harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
    # db.harvests.update_one({'harvest_date': harvest_date}, {'$unset': {'reasons': ''}})
    # db.harvests.update_one({'harvest_date': harvest_date}, {'$unset': {'total_reasons': ''}})
    pipeline = [
        {"$match": {'harvests': harvest_date}},
        {"$unwind": "$reasons"},
        {"$group": {"_id": "$reasons", "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "reason": "$_id", "total": "$total"}},
        {'$sort': {'reason': 1}}
    ]
    total_reasons = list(db.items.aggregate(pipeline))
    agg = {'harvest_date': harvest_date, 'agg_type': 'reason_totals'}
    agg['results'] = total_reasons
    db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'reason_totals'}, agg, upsert=True)
    # db.harvests.update_one({'harvest_date': harvest_date}, {'$set': {'total_reasons': total_reasons}}, upsert=False)
    for result in total_reasons:
        reason = {'harvest_date': harvest_date, 'agg_type': 'reason', 'reason': result['reason'], 'total': result['total']}
        pipeline = [
            {"$match": {'harvests': harvest_date, "reasons": result['reason']}},
            {"$group": {"_id": {"$year": "$contents_dates.start_date.date"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
            {'$sort': {'year': 1}}
        ]
        reason['start_totals'] = list(db.items.aggregate(pipeline))
        earliest_year = reason['start_totals'][0]['year']
        pipeline = [
            {"$match": {'harvests': harvest_date, "reasons": result['reason']}},
            {"$group": {"_id": {"$year": "$contents_dates.end_date.date"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
            {'$sort': {'year': 1}}
        ]
        reason['end_totals'] = list(db.items.aggregate(pipeline))
        latest_year = reason['end_totals'][-1]['year']
        year_totals = []
        for year in range(earliest_year, latest_year + 1):
            content_date = datetime(year, 1, 1, 0, 0, 0)
            total = db.items.find({'harvests': harvest_date, "reasons": result['reason'], 'contents_dates.start_date.date': {'$lte': content_date}, 'contents_dates.end_date.date': {'$gte': content_date}}).count()
            if total > 0:
                year_totals.append({'year': year, 'total': total})
        reason['year_totals'] = year_totals
        pipeline = [
            {"$match": {'harvests': harvest_date, "reasons": result['reason']}},
            {"$group": {"_id": {"$year": "$access_decision.start_date.date"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
            {'$sort': {'year': 1}}
        ]
        reason['decisions'] = list(db.items.aggregate(pipeline))
        pipeline = [
            {"$match": {'harvests': harvest_date, "reasons": result['reason']}},
            {"$group": {"_id": {"series": "$series", "title": "$series_title"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "series": "$_id.series", "title": "$_id.title", "total": "$total"}},
            {'$sort': {'total': -1}}
        ]
        reason['series'] = list(db.items.aggregate(pipeline))
        now = harvest_date.year
        bad_year = datetime(1800, 12, 31)
        pipeline = [
            {"$match": {'harvests': harvest_date, "reasons": reason['reason'], 'contents_dates.start_date.date': {"$gt": bad_year}, 'contents_dates.end_date.date': {"$gt": bad_year}}},
            {"$group": {"_id": None, "age": {"$avg": {"$subtract": [now, {"$year": "$contents_dates.start_date.date"}]}}}}
        ]
        try:
            reason['average_age'] = list(db.items.aggregate(pipeline))[0]['age']
        except IndexError:
            # There's at least one series with no good dates at all
            reason['average_age'] = None
        db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'reason', 'reason': result['reason']}, reason, upsert=True)


def aggregate_series(harvest):
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    dates = harvest.split('-')
    harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
    pipeline = [
        {"$match": {"harvests": harvest_date}},
        {"$group": {"_id": "$series", "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "series": "$_id", "total": "$total"}},
        {"$sort": {"total": -1}}
    ]
    agg = {'harvest_date': harvest_date, 'agg_type': 'series_totals'}
    series_totals = list(db.items.aggregate(pipeline))
    for index, result in enumerate(series_totals):
        series = db.series.find_one({'identifier': result['series']})
        if series:
            series_totals[index]['title'] = series['title']
        else:
            series_totals[index]['title'] = ''
    agg['results'] = series_totals
    db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'series_totals'}, agg, upsert=True)
    for result in series_totals:
        print result['series']
        series = {'harvest_date': harvest_date, 'agg_type': 'series', 'series': result['series'], 'total': result['total']}
        pipeline = [
            {"$match": {'harvests': harvest_date, 'series': result['series']}},
            {"$unwind": "$reasons"},
            {"$group": {"_id": "$reasons", "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "reason": "$_id", "total": "$total"}},
            {'$sort': {'reason': 1}}
        ]
        series['reasons'] = list(db.items.aggregate(pipeline))
        pipeline = [
            {"$match": {'harvests': harvest_date, "series": result['series']}},
            {"$group": {"_id": {"$year": "$contents_dates.start_date.date"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
            {'$sort': {'year': 1}}
        ]
        series['start_totals'] = list(db.items.aggregate(pipeline))
        earliest_year = series['start_totals'][0]['year']
        print earliest_year
        pipeline = [
            {"$match": {'harvests': harvest_date, "series": result['series']}},
            {"$group": {"_id": {"$year": "$contents_dates.end_date.date"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
            {'$sort': {'year': 1}}
        ]
        series['end_totals'] = list(db.items.aggregate(pipeline))
        latest_year = series['end_totals'][-1]['year']
        print latest_year
        year_totals = []
        for year in range(earliest_year, latest_year + 1):
            content_date = datetime(year, 1, 1, 0, 0, 0)
            total = db.items.find({'harvests': harvest_date, 'series': result['series'], 'contents_dates.start_date.date': {'$lte': content_date}, 'contents_dates.end_date.date': {'$gte': content_date}}).count()
            if total > 0:
                year_totals.append({'year': year, 'total': total})
        series['year_totals'] = year_totals
        pipeline = [
            {"$match": {'harvests': harvest_date, "series": result['series']}},
            {"$group": {"_id": {"$year": "$access_decision.start_date.date"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
            {'$sort': {'year': 1}}
        ]
        series['decisions'] = list(db.items.aggregate(pipeline))
        now = harvest_date.year
        bad_year = datetime(1800, 12, 31)
        pipeline = [
            {"$match": {'harvests': harvest_date, "series": result['series'], 'contents_dates.start_date.date': {"$gt": bad_year}, 'contents_dates.end_date.date': {"$gt": bad_year}}},
            {"$group": {"_id": None, "age": {"$avg": {"$subtract": [now, {"$year": "$contents_dates.start_date.date"}]}}}}
        ]
        try:
            series['average_age'] = list(db.items.aggregate(pipeline))[0]['age']
        except IndexError:
            # There's at least one series with no good dates at all
            series['average_age'] = None
        db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'series', 'series': result['series']}, series, upsert=True)


def aggregate_years(harvest):
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    dates = harvest.split('-')
    harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
    agg = {'harvest_date': harvest_date, 'agg_type': 'start_totals'}
    pipeline = [
        {"$match": {'harvests': harvest_date}},
        {"$group": {"_id": {"$year": "$contents_dates.start_date.date"}, "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
        {'$sort': {'year': 1}}
    ]
    agg['results'] = list(db.items.aggregate(pipeline))
    db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'start_totals'}, agg, upsert=True)
    agg = {'harvest_date': harvest_date, 'agg_type': 'end_totals'}
    pipeline = [
        {"$match": {'harvests': harvest_date}},
        {"$group": {"_id": {"$year": "$contents_dates.end_date.date"}, "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
        {'$sort': {'year': 1}}
    ]
    agg['results'] = list(db.items.aggregate(pipeline))
    db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'end_totals'}, agg, upsert=True)
    agg = {'harvest_date': harvest_date, 'agg_type': 'year_totals'}
    results = []
    for year in range(1800, harvest_date.year + 1):
        content_date = datetime(year, 1, 1, 0, 0, 0)
        total = db.items.find({'contents_dates.start_date.date': {'$lte': content_date}, 'contents_dates.end_date.date': {'$gte': content_date}}).count()
        results.append({'year': year, 'total': total})
    agg['results'] = results
    db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'year_totals'}, agg, upsert=True)
    agg = {'harvest_date': harvest_date, 'agg_type': 'series_ages'}
    now = harvest_date.year
    bad_year = datetime(1800, 12, 31)
    pipeline = [
        {"$match": {"harvests": harvest_date, 'contents_dates.start_date.date': {"$gt": bad_year}, 'contents_dates.end_date.date': {"$gt": bad_year}}},
        {"$group": {"_id": "$series", "age": {"$avg": {"$subtract": [now, {"$year": "$contents_dates.start_date.date"}]}}}},
        {"$project": {"_id": 0, "series": "$_id", "age": "$age"}},
        {"$sort": {"age": -1}}
    ]
    agg['results'] = list(db.items.aggregate(pipeline))
    db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'series_ages'}, agg, upsert=True)


def aggregate_decisions(harvest):
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    dates = harvest.split('-')
    harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
    agg = {'harvest_date': harvest_date, 'agg_type': 'decision_totals'}
    pipeline = [
        {"$match": {'harvests': harvest_date}},
        {"$group": {"_id": {"$year": "$access_decision.start_date.date"}, "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
        {'$sort': {'year': 1}}
    ]
    decision_totals = list(db.items.aggregate(pipeline))
    agg['results'] = decision_totals
    db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'decision_totals'}, agg, upsert=True)
    for result in decision_totals:
        print result
        start_date = datetime(result['year'], 1, 1, 0, 0, 0)
        end_date = datetime(result['year'], 12, 31, 0, 0, 0)
        details = {'harvest_date': harvest_date, 'agg_type': 'decision', 'decision_year': result['year'], 'total': result['total']}
        pipeline = [
            {"$match": {'harvests': harvest_date, 'access_decision.start_date.date': {"$gte": start_date, "$lte": end_date}}},
            {"$group": {"_id": {"$month": "$access_decision.start_date.date"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "month": "$_id", "total": "$total"}},
            {'$sort': {'month': 1}}
        ]
        details['months'] = list(db.items.aggregate(pipeline))
        print details['months']
        pipeline = [
            {"$match": {'harvests': harvest_date, 'access_decision.start_date.date': {"$gte": start_date, "$lte": end_date}}},
            {"$unwind": "$reasons"},
            {"$group": {"_id": "$reasons", "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "reason": "$_id", "total": "$total"}},
            {'$sort': {'reason': 1}}
        ]
        details['reasons'] = list(db.items.aggregate(pipeline))
        pipeline = [
            {"$match": {'harvests': harvest_date, 'access_decision.start_date.date': {"$gte": start_date, "$lte": end_date}}},
            {"$group": {"_id": {"series": "$series", "title": "$series_title"}, "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "series": "$_id.series", "title": "$_id.title", "total": "$total"}},
            {'$sort': {'total': -1}}
        ]
        details['series'] = list(db.items.aggregate(pipeline))
        now = harvest_date.year
        bad_year = datetime(1800, 12, 31)
        pipeline = [
            {"$match": {'harvests': harvest_date, 'access_decision.start_date.date': {"$gte": start_date, "$lte": end_date}, 'contents_dates.start_date.date': {"$gt": bad_year}, 'contents_dates.end_date.date': {"$gt": bad_year}}},
            {"$group": {"_id": None, "age": {"$avg": {"$subtract": [now, {"$year": "$contents_dates.start_date.date"}]}}}}
        ]
        try:
            details['average_age'] = list(db.items.aggregate(pipeline))[0]['age']
        except IndexError:
            # There's at least one series with no good dates at all
            details['average_age'] = None
        db.aggregates.replace_one({'harvest_date': harvest_date, 'agg_type': 'decision', 'decision_year': result['year']}, details, upsert=True)


def add_totals(harvest):
    '''Just add total items to harvest for convenience'''
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    dates = harvest.split('-')
    harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
    total = db.items.find({'harvests': harvest_date}).count()
    pipeline = [
        {"$match": {'harvests': harvest_date}},
        {"$unwind": "$reasons"},
        {"$group": {"_id": "$reasons"}},
    ]
    total_reasons = len(list(db.items.aggregate(pipeline)))
    pipeline = [
        {"$match": {'harvests': harvest_date}},
        {"$group": {"_id": "$series"}},
    ]
    total_series = len(list(db.items.aggregate(pipeline)))
    pipeline = [
        {"$match": {'harvests': harvest_date}},
        {"$group": {"_id": {"$year": "$contents_dates.start_date.date"}, "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "year": "$_id", "total": "$total"}},
        {'$sort': {'year': 1}}
    ]
    years = list(db.items.aggregate(pipeline))
    count = 0
    total_age = 0
    now = harvest_date.year
    for result in years:
        if result['year'] != 1800:
            total_age += (now - result['year']) * result['total']
            count += result['total']
    average_age = total_age / count
    db.harvests.update_one({'harvest_date': harvest_date}, {'$set': {'total': total, 'total_reasons': total_reasons, 'total_series': total_series, 'average_age': average_age}}, upsert=False)


def get_missing_series(harvest):
    '''Harvest missed some series because they're not accessible through
    RecordSearch search! Modified RS Tools to access via a direct url.'''
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    dates = harvest.split('-')
    harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
    results = db.aggregates.find_one({'harvest_date': harvest_date, 'agg_type': 'series_totals'})['results']
    rs_client = RSSeriesClient()
    for result in results:
        saved = db.series.find_one({'identifier': result['series']})
        if not saved:
            print result['series']
            try:
                series = rs_client.get_summary(entity_id=result['series'])
                agencies = rs_client.get_controlling_agencies(entity_id=result['series'])
                series['controlling_agencies'] = agencies
                db.series.insert_one(series)
            except UsageError:
                print 'Not found'
