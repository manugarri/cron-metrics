import psycopg2
import pandas as pd

from utils.config import get_io_config

env = get_io_config('redshift')
DB_USER, DB_PWD, DB_HOST, DB_NAME = env.DB_USER, env.DB_PWD, env.DB_HOST, env.DB_NAME

class RedshiftReader(object):
    '''Write to a postgres database'''

    def __init__(self, module, custom_settings=None):
        rs_conn_str = " dbname='{}' user='{}' host='{}' port='5439' password='{}'".format(
                DB_NAME, DB_USER,DB_HOST, DB_PWD)
        self.module = module

        print('INIT POSTGRES READER FOR MODULE {}'.format(module))
        self.conn = psycopg2.connect(rs_conn_str)

    def read(self, query):
        df =  pd.io.sql.read_sql(query, self.conn)
        return df

    def run(self, query):
        print('REDSHIFT READER RUN QUERY:{}'.format(query))
        cur = self.conn.cursor()
        cur.execute(query)
