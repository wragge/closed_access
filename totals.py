import re
import csv
from pymongo import MongoClient
from credentials import MONGOLAB_URL
import plotly.plotly as py
from plotly.graph_objs import *
from operator import itemgetter
from recordsearch_tools.client import RSSeriesClient, UsageError
import datetime
from dateutil.parser import *
import pprint


def get_db():
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    return db


def get_unnormalised_totals(start, end):
    """
    Get total closed files for period and broken down by reasons and reason groups.
    Use unnormalised reasons because I'm trying to match up with Annual report results
    """
    db = get_db()
    harvest_date = datetime.datetime(2016, 1, 1)
    start_date = parse(start)
    end_date = parse(end)
    total = db.items.find({'access_decision.start_date.date': {'$gte': start_date, '$lte': end_date}, 'harvests': harvest_date}).count()
    """
    pipeline = [
        {"$match": {'harvests': harvest_date, 'access_decision.start_date.date': {"$gte": start_date, "$lte": end_date}}},
        {"$group": {"_id": "$access_reason", "total": {"$sum": 1}}}
    ]
    groups = list(db.items.aggregate(pipeline))
    """
    pipeline = [
        {"$match": {'harvests': harvest_date, 'access_decision.start_date.date': {"$gte": start_date, "$lte": end_date}}},
        {"$unwind": "$access_reason"},
        {"$group": {"_id": "$access_reason.reason", "total": {"$sum": 1}}}
    ]
    reasons = list(db.items.aggregate(pipeline))
    pending = 0
    for reason in reasons:
        if reason['_id'] == 'Withheld pending adv':
            pending = reason['total']
            break
    pp = pprint.PrettyPrinter(indent=4)
    print 'TOTAL: {}'.format(total)
    print 'TOTAL (without pending advice): {}'.format(total - pending)
    # print 'GROUPS:'
    # pp.pprint(groups)
    print 'REASONS:'
    pp.pprint(reasons)


def get_financial_years(start, end):
    for year in range(start, end + 1):
        print '\n\nFINANCIAL YEAR {}-{}\n'.format(year, year + 1)
        get_unnormalised_totals('{}-07-01'.format(year), '{}-06-30'.format(year + 1))

