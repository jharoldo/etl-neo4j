import pandas as pd
import boto3
import psycopg2 as psy

from neo4j import GraphDatabase


class RedshiftConnection(object):

    def __enter__(self):
        # make a database connection and return it
        self.sql_conn = psy.connect(user=user,
                                    password=password,
                                    dbname=dbname,
                                    host=host,
                                    port=port)

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
        self.neo_conn = Neo4jConnection(uri="bolt://localhost:7687",
                                        user="superman",
                                        pwd="pizza")

        return self.neo_conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.neo_conn.close()


def openfile(filepath):
    file_content = open(filepath, 'r')
    file_content = file_content.read()
    return file_content


def extract():
    pass


def create_csv_file():
    pass
