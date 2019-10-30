import config
from peewee import *
from db import TABLE, get_connection

MIN_SIZE = int(config.get('cluster', 'min_size'))
MAX_SIZE = int(config.get('cluster', 'max_size'))
COLUMNS = tuple(s.strip() for s in config.get('cluster', 'columns').split(','))

__all__ = ['load_clusters_info', 'ClusterType', 'Cluster', 'ProcessedCluster', 'get_data']

DB_NAME = 'clusters.db'


class BaseModel(Model):
    class Meta:
        database = SqliteDatabase(DB_NAME)


class ClusterType(BaseModel):
    name = CharField(null=False, unique=True)


class Cluster(BaseModel):
    name = CharField(null=False, unique=True)
    size = IntegerField(null=False)
    type = ForeignKeyField(ClusterType, null=False)


class ProcessedCluster(BaseModel):
    started = DateTimeField(null=True)
    finished = DateTimeField(null=False)
    success = BooleanField(default=True)
    duplicates = IntegerField(default=0)
    cluster = ForeignKeyField(Cluster, null=False)


def get_cluster_query(columns=COLUMNS, min_size=MIN_SIZE, max_size=MAX_SIZE):
    query = ''
    sub_columns = []
    for column in columns:
        sub_query = rf"""SELECT {column} as name, count(*) as size, '{column}' as type
                            FROM {TABLE}
                            WHERE {column} IS NOT NULL"""
        for sub_column in sub_columns:
            sub_query += f'\n AND {sub_column} IS NULL'
        sub_query += f'\n GROUP BY {column} \n'
        query += sub_query if not query else "\nUNION\n" + sub_query
        sub_columns.append(column)
    query = f'SELECT c.* FROM ({query}) c WHERE c.size>={min_size} AND c.size<={max_size}'
    return query


def load_clusters_info(show=False):
    Cluster.delete().execute()
    ClusterType.delete().execute()
    ProcessedCluster.delete().execute()
    types = {c: ClusterType.create(name=c) for c in COLUMNS}
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(get_cluster_query())
        for row in cursor:
            show and print("cluster: ", row)
            try:
                Cluster.create(name=row['name'], size=row['size'], type=types[row['type']])
            except IntegrityError:
                print("NOT UNIQUE", row)
    conn.close()


def get_data(cluster):
    conn = get_connection()
    result = None
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {TABLE} WHERE {cluster.type.name}='{cluster.name}'")
        result = tuple(cursor)
    conn.close()
    return result


def init():
    db = SqliteDatabase(DB_NAME)
    db.connect()
    db.create_tables([ClusterType, Cluster, ProcessedCluster], safe=True)
    db.close()


init()
