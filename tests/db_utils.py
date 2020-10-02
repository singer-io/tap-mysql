import pymysql
from pymysql.constants import CLIENT

def get_db_connection(props, creds):
    connection = pymysql.connect(host=props['host'],
                                 database=props.get('database'),
                                 port=int(props['port']),
                                 user=props['user'],
                                 password=creds['password'],
                                 autocommit=True,
                                 client_flag=CLIENT.MULTI_STATEMENTS)

    with connection.cursor() as cur:
        cur.execute('SET @@session.time_zone="+0:00"')
    return connection
