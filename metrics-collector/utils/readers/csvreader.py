import pandas as pd

class CsvReader(object):
    '''Write to a csv file'''
    def __init__(self, module):
        self.module = module

    def read(self, filename):
        df = pd.read_table(filename, sep=',', dtypes='O', encoding='utf-8')
        return df
