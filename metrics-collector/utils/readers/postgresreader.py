from db import DB

from utils.config import get_io_config
env = get_io_config('redshift')
DB_USER, DB_PWD, DB_HOST, DB_NAME, DB_PORT= env.DB_USER, env.DB_PWD, env.DB_HOST, env.DB_NAME, env.DB_PORT

class PostgresReader(object):
    '''Write to a postgres database'''

    def __init__(self, module, custom_settings=None):
        reader_settings = {'username':DB_USER,
                               'password':DB_PWD,
                               'hostname':DB_HOST,
                               'dbname':DB_NAME,
                               'dbtype':'redshift',
                               'schemas':['']
                               }
        if custom_settings:
            reader_settings.update(custom_settings)

        self.module = module
        print('INIT POSTGRES READER FOR MODULE {}'.format(module))
        self.db =  DB(**reader_settings)

    def read(self, query):
        df = self.db.query(query)
        return df
