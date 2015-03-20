import os
import sys

import psycopg2
from sqlalchemy import create_engine
from db import DB

from utils.config import get_io_config

env = get_io_config('redshift')
DB_USER, DB_PWD, DB_HOST, DB_NAME, S3_BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY = env.DB_USER, env.DB_PWD, env.DB_HOST, env.DB_NAME, env.S3_BUCKET, env.AWS_ACCESS_KEY, env.AWS_SECRET_KEY

class RedshiftWriter(object):
    '''Write to a postgres database'''

    def __init__(self, module):
        self.module = module

        sys.stdout.write('{} INIT REDSHIFT WRITER FOR MODULE {}:\nDATABASE: {} HOST: {}'.format(__name__,
            module, DB_NAME, DB_HOST))

        rs_settings =  {
            'username':DB_USER,
            'password':DB_PWD,
            'hostname':DB_HOST,
            'dbname':DB_NAME,
                }

        rs_conn_str = " dbname='{dbname}' user='{username}' host='{hostname}' port='5439' password='{password}'".format(
                **rs_settings)

        rs_sqlalchemy_str = 'postgresql://{username}:{password}@{hostname}:5439/{dbname}'.format(
                **rs_settings)

        self.conn = psycopg2.connect(rs_conn_str)
        self.engine = create_engine(rs_sqlalchemy_str)

        sys.stdout.write('{} INIT DB.PY READER FOR MODULE {}'.format(__name__, module))
        rs_settings['dbtype'] = 'redshift'
        rs_settings['schemas'] = ['']
        self.db =  DB(**rs_settings)

    def _try_command(self, cmd):
        try:
            cur = self.conn.cursor()
            cur.execute(cmd)
        except Exception as e:
            sys.stderr.write("Error executing command:")
            sys.stderr.write("\t '{0}'".format(cmd))
            sys.stderr.write("Exception: {0}".format(e))
            self.conn.rollback()

    def write(self, df, table, drop_if_exists=False, s3_copy=False, **kw):
        sys.stdout.write('{}:WRITE CALL.Table:,\
                    {} Rows: {} Columns: {}'.format(
            __name__, table, df.shape[0], df.shape[1]))

        if os.environ.get('DEBUG_MODE') == 'true':
            sys.stdout.write('{}: DEBUG MODE, NOT WRITING'.format(__name__))
            return None
        if s3_copy:
            sys.stdout.write('{}: WRITING S3 COPY'.format(__name__))
            self._write_s3_copy(table, df, drop_if_exists, **kw)
            return None

        if drop_if_exists:
            sql = 'BEGIN;DELETE FROM {};COMMIT;END;'.format(table)
            self._try_command(sql)
            df.to_sql(table, self.engine,if_exists='append',index=False)
            sys.stdout.write('{}: ANALYZE AND VACUUM TABLE {}'.format(__name__, table))
            sql = 'VACUUM {};'.format(table)
            self._try_command(sql)
            sql = 'ANALYZE {};'.format(table)
            self._try_command(sql)
        else:
            sys.stdout.write('{}: BATCH INSERT'.format(__name__))
            df.to_sql(table, self.engine,if_exists='append',index=False)

    def _write_s3_copy(self, name, df, drop_if_exists=False, chunk_size=10000,
                    s3=None, print_sql=True, bucket_location=None):
        try:
            from boto.s3.connection import S3Connection
            from boto.s3.key import Key
            from boto.s3.connection import Location
            import threading
            import gzip
            from StringIO import StringIO
            import pandas as pd
            if bucket_location is None:
                bucket_location = Location.DEFAULT

        except ImportError:
            raise Exception("Couldn't find boto library. Please ensure it is installed")
        s3_bucket = S3_BUCKET
        conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = conn.get_bucket(s3_bucket)
        bucket_name = s3_bucket

        # we're going to chunk the file into pieces. according to amazon, this is
        # much faster when it comes time to run the \COPY statment.
        #
        # see http://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html
        sys.stdout.write("Transfering {0} to s3 in chunks".format(name))
        len_df = len(df)
        chunks = range(0, len_df, chunk_size)
        def upload_chunk(i):
            chunk = df[i:(i+chunk_size)]
            k = Key(bucket)
            k.key = 'data-{}-{}-{}.csv.gz'.format(name, i, i + chunk_size)
            k.set_metadata('parent', 'db.py')
            out = StringIO()
            with gzip.GzipFile(fileobj=out, mode="w") as f:
                  f.write(chunk.to_csv(index=False, encoding='utf-8'))
            k.set_contents_from_string(out.getvalue())
            sys.stdout.write(".")
            return i

        threads = []
        for i in chunks:
            t = threading.Thread(target=upload_chunk, args=(i, ))
            t.start()
            threads.append(t)

        # join all threads
        for t in threads:
            t.join()
        sys.stdout.write("done\n")

        if drop_if_exists:
            #sql = "DROP TABLE IF EXISTS {0};".format(name)
            sql = 'BEGIN;DELETE FROM {};COMMIT;END;'.format(name)

            if print_sql:
                sys.stdout.write(sql + "\n")
            self._try_command(sql)

        # generate schema from pandas and then adapt for redshift
        sql = pd.io.sql.get_schema(df, name)
        # defaults to using SQLite format. need to convert it to Postgres
        sql = sql.replace("[", "").replace("]", "")
        # we'll create the table ONLY if it doens't exist
        sql = sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        if print_sql:
            sys.stdout.write(sql + "\n")
        self._try_command(sql)
        self.conn.commit()

        # perform the \COPY here. the s3 argument is a prefix, so it'll pick up
        # all of the data*.gz files we've created
        sys.stdout.write("Copying data from s3 to redshfit...")
        sql = """
        copy {name} from 's3://{bucket_name}/data-{name}'
        credentials 'aws_access_key_id={AWS_ACCESS_KEY};aws_secret_access_key={AWS_SECRET_KEY}'
        CSV IGNOREHEADER as 1 GZIP;
        """.format(name=name, bucket_name=bucket_name,
                   AWS_ACCESS_KEY=AWS_ACCESS_KEY, AWS_SECRET_KEY=AWS_SECRET_KEY)
        if print_sql:
            sys.stdout.write(sql + "\n")
        self._try_command(sql)
        self.conn.commit()
        sys.stdout.write("done!\n")
        # tear down the bucket
        sys.stdout.write("Tearing down bucket...")
        for key in bucket.list():
            if key.key.startswith('data-{}'.format(name)):
                sys.stdout.write('Deleting key {}'.format(key.key))
                key.delete()
        sys.stdout.write("done!")

