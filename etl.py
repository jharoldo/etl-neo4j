import pandas as pd
import boto3
import psycopg2 as psy
import os

import py2neo
from io import StringIO
from datetime import datetime

# Credentials from data source and data target
user_redshift = os.environ['USER_REDSHIFT']
password_redshift = os.environ['PASSWORD_REDSHIFT']
dbname_redshift = os.environ['DBNAME_REDSHIFT']
host_redshift = os.environ['HOST_REDSHIFT']
port_redshift = os.environ['PORT_REDSHIFT']
host_neo4j = os.environ['HOST_NEO4J']
user_neo4j = os.environ['USER_NEO4J']
password_neo4j = os.environ['PASSWORD_NEO4J']
dbname_neo4j = os.environ['DBNAME_NEO4J']
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']

# Paths files
extract_query = ''
path_s3 = ''
bucket_name = ''


class RedshiftConnection(object):

    def __enter__(self):
        # make a database connection and return it
        self.sql_conn = psy.connect(user=user_redshift,
                                    password=password_redshift,
                                    dbname=dbname_redshift,
                                    host=host_redshift,
                                    port=port_redshift)

        return self.sql_conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.sql_conn.close()


def connection_neo4j():
    neo_conn = py2neo.Graph(host_neo4j,
                            user=user_neo4j,
                            password=password_neo4j,
                            database=dbname_neo4j)
    return neo_conn


def openfile(filepath):
    file_content = open(filepath, 'r')
    return file_content.read()


def extract(path_query):
    extract_query_str = openfile(path_query)
    conn = connection_neo4j()
    result = conn.run(extract_query_str).to_data_frame()
    return result


def create_csv_file(df, aws_access_key, aws_secret_key, bucket, path_file_s3):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    data = datetime.today().strftime('%Y%m%d')
    filepaths3 = path_file_s3.format(data)
    s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepaths3)


def main():
    df = extract(extract_query, dbname_neo4j)
    create_csv_file(df, aws_access_key, aws_secret_key, bucket_name, path_s3)


if __name__ == '__main__':
    main()
