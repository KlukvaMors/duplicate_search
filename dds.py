'''dds - database duplicate search'''

from fuzzywuzzy import fuzz
from statistics import mean
from cluster import get_data, Cluster, ProcessedCluster
from datetime import datetime
from peewee import *
import config
import json

TOLERANCE = int(config.get('dds', 'tolerance'))  # from 0 to 100
EXACT_MATCH_COLUMNS = tuple(s.strip() for s in config.get('dds', 'exact_match_columns').split(','))
TOLERANCE_MATCH_COLUMNS = tuple(s.strip() for s in config.get('dds', 'tolerance_match_columns').split(','))
ID = config.get('db', 'id_column')
DB_NAME = 'duplicates.db'


class Duplicate(Model):
    id1 = BigIntegerField(null=False)
    id2 = BigIntegerField(null=False)
    obj1_json = TextField(null=False)
    obj2_json = TextField(null=False)

    class Meta:
        database = SqliteDatabase(DB_NAME)


def init():
    db = SqliteDatabase(DB_NAME)
    db.connect()
    db.create_tables([Duplicate], safe=True)
    db.close()


def duplicate(obj1: dict, obj2: dict):
    if not all([obj1[key] == obj2[key] for key in EXACT_MATCH_COLUMNS]):
        return False
    avg = mean([fuzz.token_set_ratio(obj1[key], obj2[key]) for key in TOLERANCE_MATCH_COLUMNS])
    return avg >= TOLERANCE


def process_cluster(cluster):
    data = get_data(cluster)
    start = datetime.now()
    num_duplicate = 0
    for i, row in enumerate(data):
        for j in range(i+1, len(data)):
            if duplicate(row, data[j]):
                num_duplicate += 1
                Duplicate.create(
                    id1=row[ID],
                    id2=data[j][ID],
                    obj1_json=json.dumps(dict(row), default=str),
                    obj2_json=json.dumps(dict(data[j]), default=str)
                )
    finish = datetime.now()
    ProcessedCluster.create(started=start, finished=finish, cluster=cluster, duplicates=num_duplicate)


init()

if __name__=='__main__':
    for cluster in Cluster.select().limit(10):
        process_cluster(cluster)

