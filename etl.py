import pandas as pd
import boto3
import psycopg2 as psy
import os

from neo4j import GraphDatabase

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

# Query path files
extract_query = 'cypher-queries/extract-query.txt'


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


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


class DbNeo4jConnection(object):

    def __enter__(self):
        # make a database connection and return it
        self.neo_conn = Neo4jConnection(uri=host_neo4j,
                                        user=user_neo4j,
                                        pwd=user_neo4j)

        return self.neo_conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.neo_conn.close()


def openfile(filepath):
    file_content = open(filepath, 'r')
    file_content = file_content.read()
    return file_content


def extract(path_query, db_name=None):
    extract_query_str = openfile(path_query)
    with DbNeo4jConnection() as neo_conn:
        result = neo_conn.query(extract_query_str, db=db_name)
    df_result = pd.DataFrame([dict(_) for _ in result])
    return df_result


def create_csv_file():
    pass


