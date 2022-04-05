import pyodbc
import conn_str as db


class azure_conn(object):
    def __init__(self, connection_str=db.conn_str):
        self.connection_str = connection_str
        self.connector = None
        self.cursor = None

    def __enter__(self):
        self.connector = pyodbc.connect(self.connection_str)
        self.cursor = self.connector.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.connector.commit()
            self.cursor.close()
        else:
            self.connector.rollback()
        self.connector.close()


class azure_sql(object):
    def __init__(self, connection_str=db.conn_str):
        self.connection_str = connection_str
        self.connector = None

    def __enter__(self):
        self.connector = pyodbc.connect(self.connection_str)
        return self.connector

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()
