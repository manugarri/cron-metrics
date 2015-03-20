from utils.readers.csvreader import CsvReader
from utils.readers.postgresreader import PostgresReader
from utils.readers.redshifteader import RedshiftReader
from utils.readers.dbpyreader import DBPYReader

READER_DICT = {
    'csv':CsvReader,
    'postgres':PostgresReader,
    'redshift':RedshiftReader,
    'dbpy':DBPYReader
    }


def init_reader(module, reader_type='redshift', **kw):
    return READER_DICT[reader_type](module, **kw)
