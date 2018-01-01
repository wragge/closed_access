# Look at how old files were when decisions were made.

from pymongo import MongoClient
from credentials import MONGOLAB_URL
import datetime
from dateutil.relativedelta import relativedelta
import collections

harvest_date = datetime.datetime(2016, 1, 1)


def get_db():
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    return db


def get_age_at_decision(age):
    db = get_db()
    total = 0
    reasons = {}
    for year in range(1966, 2016):
        start_date = datetime.datetime(year, 1, 1)
        end_date = datetime.datetime(year, 12, 31)
        content_date = datetime.datetime(year - int(age), 12, 31)
        # age = decision['date'] - relativedelta(years=int(age))
        total += db.items.find({'access_decision.start_date.date': {'$gte': start_date, '$lte': end_date}, 'contents_dates.end_date.date': {'$gte': datetime.datetime(1800, 12, 31), '$lte': content_date}, 'reasons': {'$ne': ['Parliament Class A']}}).count()
        pipeline = [
            {'$match': {'access_decision.start_date.date': {'$gte': start_date, '$lte': end_date}, 'contents_dates.end_date.date': {'$gte': datetime.datetime(1800, 12, 31), '$lte': content_date}}},
            {'$unwind': '$reasons'},
            {'$group': {'_id': '$reasons', 'total': {'$sum': 1}}},
            {'$project': {'_id': 0, 'reason': '$_id', 'total': '$total'}}
        ]
        results = list(db.items.aggregate(pipeline))
        for reason in results:
            if reason['reason'] != 'Parliament Class A':
                try:
                    reasons[reason['reason']] += reason['total']
                except KeyError:
                    reasons[reason['reason']] = reason['total']
    print 'Age: {}'.format(age)
    print 'Total: {}'.format(total)
    reasons = collections.OrderedDict(sorted(reasons.items()))
    for reason, rtotal in reasons.iteritems():
        print '| {} | {} | {:.2%} |'.format(reason, rtotal, rtotal / float(total))


def get_all_ages():
    for age in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]:
        get_age_at_decision(age)


def get_most_conservative(reason):
    db = get_db()
    pipeline = [
        {"$match": {'harvests': harvest_date, 'reasons': reason, 'contents_dates.end_date.date': {'$gt': datetime.datetime(1800, 12, 31)}}},
        {'$project': {'identifier': 1, 'series': 1, 'control_symbol': 1, 'title': 1, 'reasons': 1, 'contents_dates': 1, 'access_decision': 1, 'age': {'$subtract': ['$access_decision.start_date.date', '$contents_dates.end_date.date']}}},
        {'$sort': {'age': -1}},
        {'$limit': 50}
    ]
    items = list(db.items.aggregate(pipeline))
    for item in items:
        age = datetime.timedelta(milliseconds=item['age']).days / 365
        print '{}, {}, {} -- {} years (between {} and {})'.format(item['series'], item['identifier'], item['title'], age, item['access_decision']['start_date']['date'].year, item['contents_dates']['end_date']['date'].year)
