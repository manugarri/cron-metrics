from utils.writers.csvwriter import CsvWriter
from utils.writers.postgreswriter import PostgresWriter
from utils.writers.redshiftwriter import RedshiftWriter


WRITER_DICT = {
    'csv':CsvWriter,
    'postgres':PostgresWriter,
    'redshift': RedshiftWriter
    }

def init_writer(module, writer_type, **kw):
    return WRITER_DICT[writer_type](module, **kw)
