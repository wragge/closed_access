import time
from recordsearch_tools.client import RSSearchClient, RSItemClient, RSSeriesClient, UsageError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from credentials import MONGOLAB_URL_2017, MONGOLAB_URL_2016, MONGOLAB_URL, MONGOLAB_URL_2018
import random
import re
from datetime import datetime
import csv
from harvest_closed import EXCEPTIONS
import plotly.plotly as py
import plotly.graph_objs as go


def find_changes(old=MONGOLAB_URL_2017, new=MONGOLAB_URL_2018):
    # dbclient2016 = MongoClient(MONGOLAB_URL_2016)
    # db2016 = dbclient2016.get_default_database()
    old_client = MongoClient(old)
    old_db = old_client.get_default_database()
    item_client = RSItemClient()
    new_client = MongoClient(new)
    new_db = new_client.get_default_database()
    for item_old in old_db.items.find().batch_size(50):
        barcode = item_old['identifier']
        print barcode
        exists = new_db.changes.find_one({'_id': barcode})
        if not exists:
            item_new = new_db.items.find_one({'_id': barcode})
            if item_new is None:
                try:
                    item_current = item_client.get_summary(entity_id=barcode)
                except UsageError:
                    item_old['update'] = {'access_status': 'Missing'}
                    print 'Missing'
                else:
                    print item_current['access_status']
                    item_current['reasons'] = []
                    for reason in item_current['access_reason']:
                        matched = False
                        for exception, pattern in EXCEPTIONS:
                            if re.match(pattern, reason['reason']):
                                item_current['reasons'].append(exception)
                                matched = True
                        if not matched:
                            item_current['reasons'].append(reason['reason'])
                    item_old['update'] = {'access_status': item_current['access_status']}
                    item_old['current'] = item_current
                new_db.changes.insert_one(item_old)


# ANALYSE CHANGES

def save_changed_data(dbase=MONGOLAB_URL_2018, access_status=None, reason=None, series=None, decision_year=None):
    client = MongoClient(dbase)
    db = client.get_default_database()
    query = {}
    title = 'data/records-changed'
    if access_status:
        query['current.access_status'] = access_status
        title = '{}-{}'.format(title, access_status.lower())
    if reason:
        query['reasons'] = reason
        title = '{}-{}'.format(title, reason)
    if series:
        query['series'] = series
        title = '{}-{}'.format(title, series)
    if decision_year:
        title = '{}-{}'.format(title, decision_year)
        decision_date_start = datetime(decision_year, 1, 1, 0, 0, 0)
        decision_date_end = datetime(decision_year, 12, 31, 0, 0, 0)
        query['current.access_decision.start_date.date'] = {'$gte': decision_date_start, '$lte': decision_date_end}
    records = db.changes.find(query)
    total_days = 0
    count = 0
    with open('{}.csv'.format(title), 'wb') as titles_file:
        titles = csv.writer(titles_file)
        titles.writerow([
            'barcode',
            'series',
            'control symbol',
            'title',
            'contents dates',
            'contents start year',
            'contents end year',
            'original access status',
            'original access decision reasons',
            'original access decision date',
            'current access status',
            'current access decision reasons',
            'current access decision date',
            'days closed',
            'years closed',
            'url'
            ])
        for record in records:

            period = record['current']['access_decision']['start_date']['date'] - record['access_decision']['start_date']['date']
            years, days = divmod(period.days, 365.25)
            if record['access_decision']['start_date']['date'].year != 1900 and record['current']['access_decision']['start_date']['date'].year != 1900:
                total_days += period.days
                count += 1
            titles.writerow([
                record['current']['identifier'],
                record['current']['series'],
                record['current']['control_symbol'],
                record['current']['title'].encode('utf-8'),
                record['current']['contents_dates']['date_str'].encode('utf-8'),
                record['current']['contents_dates']['start_date']['date'].year,
                record['current']['contents_dates']['end_date']['date'].year,
                record['access_status'].encode('utf-8'),
                ' | '.join(record['reasons']),
                record['access_decision']['start_date']['date'],
                record['current']['access_status'].encode('utf-8'),
                ' | '.join(record['current']['reasons']),
                record['current']['access_decision']['start_date']['date'],
                period.days,
                '{} years {} days'.format(int(years), int(round(days))),
                'http://www.naa.gov.au/cgi-bin/Search?O=I&Number={}'.format(record['current']['identifier'])
                ])
    print total_days
    average_days = total_days / count
    years, days = divmod(average_days, 365.25)
    print 'Average period closed: {} days ({} years {} days)'.format(average_days, int(years), int(round(days)))


def get_reason_changes(db):
    pipeline = [
            {"$unwind": "$reasons"},
            {"$group": {"_id": "$reasons", "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "reason": "$_id", "total": "$total"}},
            {"$sort": {"reason": 1}}
        ]
    return list(db.changes.aggregate(pipeline))


def count_changes(db=MONGOLAB_URL_2018, reason=None, series=None, decision_year=None):
    client = MongoClient(db)
    db = client.get_default_database()
    query = {}
    if reason:
        query['reasons'] = reason
    if series:
        query['series'] = series
    if decision_year:
        decision_date_start = datetime(decision_year, 1, 1, 0, 0, 0)
        decision_date_end = datetime(decision_year, 12, 31, 0, 0, 0)
        query['access_decision.start_date.date'] = {'$gte': decision_date_start, '$lte': decision_date_end}
    print query
    pipeline = [
        {'$match': query},
        {'$group': {'_id': '$update.access_status', 'total': {'$sum': 1}}},
        {'$sort': {'total': -1}}
    ]
    results = db.changes.aggregate(pipeline)
    print list(results)


def get_series(db, reason=None, decision_year=None):
    query = {}
    if reason:
        query['reasons'] = reason
    if decision_year:
        decision_date_start = datetime(decision_year, 1, 1, 0, 0, 0)
        decision_date_end = datetime(decision_year, 12, 31, 0, 0, 0)
        query['access_decision.start_date.date'] = {'$gte': decision_date_start, '$lte': decision_date_end}
    pipeline = [
        {"$match": query},
        {"$group": {"_id": "$series", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ]
    return list(db.items.aggregate(pipeline))[:20]


def get_reasons(db, decision_year=None):
    '''
    Get the normalised reasons and count.
    '''
    query = {}
    if decision_year:
        decision_date_start = datetime(decision_year, 1, 1, 0, 0, 0)
        decision_date_end = datetime(decision_year, 12, 31, 0, 0, 0)
        query['access_decision.start_date.date'] = {'$gte': decision_date_start, '$lte': decision_date_end}
    pipeline = [
            {"$match": query},
            {"$unwind": "$reasons"},
            {"$group": {"_id": "$reasons", "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "reason": "$_id", "total": "$total"}},
            {"$sort": {"reason": 1}}
        ]
    items = db.items
    return list(items.aggregate(pipeline))


def plot_reasons_changes(dbase={'year': '2017', 'db': MONGOLAB_URL_2018}):
    client = MongoClient(dbase['db'])
    db = client.get_default_database()
    reasons = get_reasons(db, 2017)
    changes = get_reason_changes(db)
    added = []
    changed = []
    x = sorted(list(set([reason['reason'] for reason in reasons] + [reason['reason'] for reason in changes])))
    for key in x:
        added.append(get_total(reasons, key))
        changed.append(0 - get_total(changes, key))
    data = [
        go.Bar(
            x=x,
            y=added,
            name='Added'
        ),
        go.Bar(
            x=x,
            y=changed,
            name='Removed'
        )
    ]
    layout = go.Layout(
        title='<b>Closed files</b><br>Changes in {}'.format(dbase['year']),
        barmode='relative',
        xaxis=go.XAxis(
            title='Reason the file is closed'
        ),
        yaxis=go.YAxis(
            title='Number of files'
        ),
        margin=dict(
            l=100,
            r=100,
            t=100,
            b=180
        ),
    )
    fig = go.Figure(data=data, layout=layout)
    plot_url = py.plot(fig, filename='totals-by-reason-changes-{}'.format(dbase['year']))


def get_total(results, key):
    total = 0
    for result in results:
        if 'reason' in result and result['reason'] == key:
            total = result['total']
            break
    return total


def plot_reasons_total_changes(dbase={'year': '2017', 'db': MONGOLAB_URL_2018}):
        client = MongoClient(dbase['db'])
        db = client.get_default_database()
        reasons = get_reasons(db, 2017)
        changes = get_reason_changes(db)
        x = sorted(list(set([reason['reason'] for reason in reasons] + [reason['reason'] for reason in changes])))
        y = []
        for key in x:
            y.append(get_total(reasons, key) - get_total(changes, key))
        data = [
            go.Bar(
                x=x,
                y=y,
                name='Added'
            )
        ]
        layout = go.Layout(
            title='<b>Closed files</b><br>Net changes in {}'.format(dbase['year']),
            barmode='relative',
            xaxis=go.XAxis(
                title='Reason the file is closed'
            ),
            yaxis=go.YAxis(
                title='Number of files'
            ),
            margin=dict(
                l=100,
                r=100,
                t=100,
                b=180
            ),
        )
        fig = go.Figure(data=data, layout=layout)
        plot_url = py.plot(fig, filename='totals-by-reason-net-changes-{}'.format(dbase['year']))



def plot_reasons_comparison(dbs=[{'year': '2016 harvest', 'db': MONGOLAB_URL_2016}, {'year': '2017 harvest', 'db': MONGOLAB_URL_2017}, {'year': '2018 harvest', 'db': MONGOLAB_URL_2018}]):
    traces = []
    years = []
    for year in dbs:
        years.append(year['year'])
        client = MongoClient(year['db'])
        db = client.get_default_database()
        reasons = get_reasons(db)
        trace = go.Bar(
            x=[reason['reason'] for reason in reasons],
            y=[reason['total'] for reason in reasons],
            name=year['year']
        )
        traces.append(trace)
    layout = go.Layout(
        title='<b>Closed files</b><br>Totals by reason, 2015-2017',
        barmode='group',
        xaxis=go.XAxis(
            title='Reason the file is closed'
        ),
        yaxis=go.YAxis(
            title='Number of files'
        ),
        margin=dict(
            l=100,
            r=100,
            t=100,
            b=150
        )
    )
    fig = go.Figure(data=traces, layout=layout)
    plot_url = py.plot(fig, filename='totals-by-reason-compared-{}'.format('-'.join(years)))


def plot_series(dbase={'year': '2016', 'db': MONGOLAB_URL_2017}, reason=None, decision_year=None):
    client = MongoClient(dbase['db'])
    db = client.get_default_database()
    series_list = get_series(db, reason=reason, decision_year=decision_year)
    if reason:
        title = '<b>Closed access</b><br>Most common series of items citing \'{}\''.format(reason)
        filename = 'closed-by-series-{}'.format(reason)
    else:
        title = '<b>Closed access</b><br>Most common series of files'
        filename = 'closed-by-series'
    if decision_year:
        title += ', {}'.format(decision_year)
        filename += '-{}'.format(decision_year)
    x = []
    y = []
    text = []
    for series in series_list:
        x.append(series['_id'])
        y.append(series['total'])
    data = go.Data([
        go.Bar(
            x=x,
            y=y
        )
    ])
    layout = go.Layout(
        title=title,
        showlegend=False,
        xaxis=go.XAxis(
            title='Series number'
        ),
        yaxis=go.YAxis(
            title='Number of files'
        ),
        bargap=0.05
    )
    fig = go.Figure(data=data, layout=layout)
    plot_url = py.plot(fig, filename=filename)
