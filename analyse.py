import re
import csv
from pymongo import MongoClient
from credentials import MONGOLAB_URL
import plotly.plotly as py
from plotly.graph_objs import *
from operator import itemgetter
from recordsearch_tools.client import RSSeriesClient, UsageError

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
    ['33(3)(b)', r'33\(3\)[ai\(\) &]*\(b\)']
]


def get_db():
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    return db


def get_reasons():
    '''
    Get the reasons applied in RS and count.
    '''
    db = get_db()
    pipeline = [
            {"$unwind": "$access_reason"},
            {"$group": {"_id": "$access_reason.reason", "total": {"$sum": 1}}},
        ]
    items = db.items
    return list(items.aggregate(pipeline))


def get_normalised_reasons():
    '''
    Get the normalised reasons and count.
    '''
    db = get_db()
    pipeline = [
            {"$unwind": "$reasons"},
            {"$group": {"_id": "$reasons", "total": {"$sum": 1}}},
            {"$project": {"_id": 0, "reason": "$_id", "total": "$total"}}
        ]
    items = db.items
    return list(items.aggregate(pipeline))


def get_relatonships():
    db = get_db()
    reasons = get_normalised_reasons()
    reasons2 = reasons[:]
    for reason in reasons:
        for reason2 in reasons2:
            if not reason['reason'] == reason2['reason']:
                count = db.items.count({"reasons": {"$all": [reason['reason'], reason2['reason']]}})
                if count > 0:
                    print '{} - {} - {}'.format(reason['reason'], reason2['reason'], count)


def set_year():
    '''
    Make the year of the contents end date available as a year integer, so I can calculate ages.
    '''
    db = get_db()
    for item in db.items.find({'year': {"$exists": False}}):
        year = int(item['contents_dates']['end_date'][:4])
        item['year'] = year
        db.items.save(item)
        print year


def get_series(reason=None, number=20):
    rsclient = RSSeriesClient()
    db = get_db()
    if reason:
        pipeline = [
            {"$match": {"reasons": reason}},
            {"$group": {"_id": "$series", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}}
        ]
    else:
        pipeline = [
            {"$group": {"_id": "$series", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}}
        ]
    items = list(db.items.aggregate(pipeline))[:number]
    for item in items:
        try:
            agencies = rsclient.get_controlling_agencies(entity_id=item['_id'], date_format='iso')
        except UsageError:
            controlling_agency = {'identifier': 'Unknown', 'title': 'Unknown'}
        for agency in agencies:
            print agency
            if not agency['end_date']:
                controlling_agency = agency
                break
        item['agency'] = controlling_agency
    return items


def get_ages():
    db = get_db()
    pipeline = [
        {"$match": {"year": {"$gte": 1900}}},
        {"$project": {"age": {"$subtract": [2015, "$year"]}}},
        {"$group": {"_id": "$age", "total": {"$sum": 1}}}
    ]
    items = db.items
    return list(items.aggregate(pipeline))


def get_reason_ages(reason):
    db = get_db()
    pipeline = [
        {"$match": {"year": {"$gte": 1900}, "reasons": reason}},
        {"$project": {"age": {"$subtract": [2015, "$year"]}}},
        {"$group": {"_id": "$age", "total": {"$sum": 1}}}
    ]
    items = db.items
    return list(items.aggregate(pipeline))


def sort_reasons(reasons):
    '''
    Normalises the reasons.
    '''
    sorted_reasons = {}
    for reason in reasons:
        matched = False
        for exception, pattern in EXCEPTIONS:
            if re.match(pattern, reason['_id']):
                matched = True
                try:
                    sorted_reasons[exception] += reason['total']
                except KeyError:
                    sorted_reasons[exception] = reason['total']
        if not matched:
            sorted_reasons[reason['_id']] = reason['total']
    return sorted_reasons


def add_reasons():
    '''
    Normalises the reasons, based on the sorting rules and writes them back to the db.
    '''
    db = get_db()
    reasons = get_reasons()
    for reason in reasons:
        matched = False
        for exception, pattern in EXCEPTIONS:
            if re.match(pattern, reason['_id']):
                matched = True
                result = db.items.update_many({'access_reason.reason': reason['_id']}, {'$push': {'reasons': exception}})
                print result.modified_count
        if not matched:
            result = db.items.update_many({'access_reason.reason': reason['_id']}, {'$push': {'reasons': reason['_id']}})
            print result.modified_count


def get_titles(reason=None, series=None, year=None):
    db = get_db()
    query = {}
    title = 'data/titles'
    if reason:
        query['reasons'] = reason
        title = '{}-{}'.format(title, reason)
    if series:
        query['series'] = series
        title = '{}-{}'.format(title, series)
    if year:
        query['year'] = year
        title = '{}-{}'.format(title, year)
    records = db.items.find(query)
    with open('{}.txt'.format(title), 'wb') as titles:
        for record in records:
            titles.write('{}\n'.format(record['title']))


def get_titles_data(reason=None, series=None, year=None):
    db = get_db()
    query = {}
    title = 'data/titles'
    if reason:
        query['reasons'] = reason
        title = '{}-{}'.format(title, reason)
    if series:
        query['series'] = series
        title = '{}-{}'.format(title, series)
    if year:
        query['year'] = year
        title = '{}-{}'.format(title, year)
    records = db.items.find(query)
    with open('{}.csv'.format(title), 'wb') as titles_file:
        titles = csv.writer(titles_file)
        for record in records:
            titles.writerow([record['identifier'], record['series'], record['control_symbol'], record['title'], record['year']])


def plot_reasons():
    reasons = get_reasons()
    sorted_reasons = sort_reasons(reasons)
    x = []
    y = []
    for reason in sorted(sorted_reasons.keys()):
        x.append(reason)
        y.append(sorted_reasons[reason])
    data = Data([
        Bar(
            x=x,
            y=y
        )
    ])
    layout = Layout(
        title='Closed files',
        showlegend=False,
        bargap=0.05,
        xaxis=XAxis(
            title='Reason for decision'
        ),
        yaxis=YAxis(
            title='Number of files'
        ),
    )
    fig = Figure(data=data, layout=layout)
    plot_url = py.plot(fig, filename='totals-by-reason')


def plot_ages(reason=None):
    if reason:
        ages = get_reason_ages(reason)
        title = 'Ages of files \'Closed\' due to \'{}\''.format(reason)
        filename = 'closed-by-age-{}'.format(reason)
    else:
        ages = get_ages()
        title = 'Ages of \'Closed\' files'
        filename = 'closed-by-age'
    x = []
    y = []
    for age in sorted(ages, key=itemgetter('_id')):
        x.append(age['_id'])
        y.append(age['total'])
    data = Data([
        Scatter(
            x=x,
            y=y
        )
    ])
    layout = Layout(
        title=title,
        showlegend=False,
        xaxis=XAxis(
            title='Age in years (based on content dates)'
        ),
        yaxis=YAxis(
            title='Number of files'
        ),
        annotations=Annotations([
            Annotation(
                x=20,
                y=0,
                xref='x',
                yref='y',
                showarrow=True,
                arrowhead=0,
                arrowsize=1,
                arrowwidth=0,
                arrowcolor="rgb(255, 127, 14)",
                text='Start of open period',
                ax=0,
                ay=-200
            )
        ])
    )
    fig = Figure(data=data, layout=layout)
    plot_url = py.plot(fig, filename=filename)


def plot_series(reason=None):
    series_list = get_series(reason=reason)
    if reason:
        title = 'Most common series of items \'Closed\' due to \'{}\''.format(reason)
        filename = 'closed-by-series-{}'.format(reason)
    else:
        title = 'Most common series of \'Closed\' files'
        filename = 'closed-by-series'
    x = []
    y = []
    text = []
    for series in series_list:
        hover = '{} {}'.format(series['agency']['identifier'], series['agency']['title'])
        if len(hover) > 80:
            hover = hover[:80] + '...'
        x.append(series['_id'])
        y.append(series['total'])
        text.append(hover)
    data = Data([
        Bar(
            x=x,
            y=y,
            text=text
        )
    ])
    layout = Layout(
        title=title,
        showlegend=False,
        xaxis=XAxis(
            title='Most common series'
        ),
        yaxis=YAxis(
            title='Number of files'
        ),
        bargap=0.05
    )
    fig = Figure(data=data, layout=layout)
    plot_url = py.plot(fig, filename=filename)

