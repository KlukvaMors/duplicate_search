import config

__all__ = ['SQL_SELECT', 'get_connection', 'TABLE']

# CONSTANTS
TABLE = config.get('db', 'table')
SQL_SELECT = rf"SELECT * FROM {TABLE}"


def get_connection(dict_result=True):
    db_params = {
        'host': config.get('db', 'host'),
        'port': config.get('db', 'port'),
        'user': config.get('db', 'user'),
        'password': config.get('db', 'password'),
    }
    if config.get('db', 'type') == 'postgres':
        import psycopg2
        from psycopg2.extras import DictCursor
        return psycopg2.connect(
            database=config.get('db', 'name'),
            cursor_factory=DictCursor if dict_result else None, #cursorclass for mysql
            **db_params
        )
    if config.get('db', 'type') == 'mysql':
        import pymysql
        from pymysql.cursors import DictCursor, Cursor
        return pymysql.connect(
            database=config.get('db', 'name'),
            charset='utf8mb4',
            cursorclass=DictCursor if dict_result else Cursor,
            **db_params
        )





