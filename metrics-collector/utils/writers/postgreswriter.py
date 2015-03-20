import os
import sys

from sqlalchemy import create_engine

from utils.config import get_io_config
env = get_io_config('redshift')
DB_USER, DB_PWD, DB_HOST, DB_NAME, DB_PORT= env.DB_USER, env.DB_PWD, env.DB_HOST, env.DB_NAME, env.DB_PORT

class PostgresWriter(object):
    '''Write to a postgres database'''

    def __init__(self, module):
        self.module = module
        pg_conn_str = "postgresql://{}:{}@{}:{}/{}".format(
                DB_USER, DB_PWD, DB_HOST, DB_PORT, DB_NAME
                )
        sys.stdout.write('INIT POSTGRES WRITER FOR MODULE {}:\nDATABASE: {} HOST: {}'.format(module,
            DB_NAME, DB_HOST))
        self.engine = create_engine(pg_conn_str)

    def write(self, df, table, if_exists='append'):
        sys.stdout.write('{}:WRITE CALL.Table:,\
                    {} Rows: {} Columns: {}'.format(
            __name__, table, df.shape[0], df.shape[1]))

        if os.environ.get('DEBUG_MODE') == 'true':
            sys.stdout.write('DEBUG MODE, NOT WRITING')
            return None
        else:
            sys.stdout.write('WRITING')
            df.to_sql(table, self.engine,if_exists=if_exists, index=False)
