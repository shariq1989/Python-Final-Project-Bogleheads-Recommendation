"""
From https://www.postgresqltutorial.com/postgresql-python/connect/
for connecting to a database using a configuration file
"""
from configparser import ConfigParser

import psycopg2


class MyDatabase:
    def __init__(self):
        pass

    def open_conn(self):
        # read connection parameters and connect
        params = config()
        conn = psycopg2.connect(**params)
        # create a cursor
        cur = conn.cursor()
        return {'conn': conn, 'cur': cur}

    def query_fetch(self, sql, param=None):
        db = self.open_conn()
        # execute a statement
        if param:
            db['cur'].execute(sql, param)
        else:
            db['cur'].execute(sql)
        recs = db['cur'].fetchone()
        self.close(db)
        return recs

    def perform_insert(self, sql, param):
        row_id = None
        try:
            db = self.open_conn()
            # execute the INSERT statement
            db['cur'].execute(sql, param)
            # get the generated id back
            row_id = db['cur'].fetchone()[0]
            # commit the changes to the database
            db['conn'].commit()
            # close communication with the database
            self.close(db)
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error occurred while inserting: ', error)
        return row_id

    def delete_rec(self, sql, param):
        db = self.open_conn()
        # insert thread
        db['cur'].execute(sql, param)
        rows_deleted = db['cur'].rowcount
        db['conn'].commit()
        self.close(db)
        return rows_deleted

    def close(self, db):
        # close the communication with the PostgreSQL
        db['cur'].close()
        db['conn'].close()


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db
