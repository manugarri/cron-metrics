import os
import sys

class CsvWriter(object):
    '''Write to a csv file'''
    def __init__(self, module):
        self.module = module
        if not os.path.exists('csv'):
            os.makedirs('csv')

    def write(self, df, table, **kw):
        sys.stdout.write('{}:WRITE CALL.Table:,\
                    {} Rows: {} Columns: {}'.format(
            __name__, table, df.shape[0], df.shape[1]))

        if os.environ.get('DEBUG_MODE') == 'true':
            sys.stdout.write('DEBUG MODE, NON WRITING')
            return None
        else:
            sys.stdout.write('WRITING')
            sys.stdout.write(os.getcwd())
            df.to_csv('./csv/' + table + '.csv', index=False, encoding='utf-8')
