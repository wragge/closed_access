# Make CSV files with nodes and edges to explore relationships between reasons.

import csv
from pymongo import MongoClient
from credentials import MONGOLAB_URL
import datetime

harvest_date = datetime.datetime(2016, 1, 1)


def get_db():
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    return db


def get_reasons():
    db = get_db()
    agg = db.aggregates.find_one({'harvest_date': harvest_date, 'agg_type': 'reason_totals'})
    reasons = [reason['reason'] for reason in agg['results']]
    return reasons


def get_edges(source, targets):
    edges = []
    for target in targets:
        edges.append([source, target, 'Undirected'])
    return edges


def make_edges():
    db = get_db()
    reasons = get_reasons()
    for item in db.items.find({'harvests': harvest_date}):
        edges = []
        nodes = [reasons.index(reason) for reason in item['reasons']]
        while len(nodes) > 1:
            source = nodes[0]
            nodes.pop(0)
            edges += get_edges(source, nodes)
        if edges:
            with open('edges.csv', 'ab') as edges_csv:
                csv_writer = csv.writer(edges_csv)
                for edge in edges:
                    csv_writer.writerow(edge)


def make_nodes():
    reasons = get_reasons()
    with open('nodes.csv', 'wb') as nodes_csv:
        csv_writer = csv.writer(nodes_csv)
        for index, reason in enumerate(reasons):
            csv_writer.writerow([index, reason])
