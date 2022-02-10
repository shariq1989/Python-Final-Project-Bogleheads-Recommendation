import psycopg2

from db.config import MyDatabase


class Link():
    def __init__(self, link, thread_id, thread_url, thread_title, domain, page):
        self.link = link
        self.thread_id = thread_id
        self.thread_url = thread_url
        self.thread_title = thread_title
        self.domain = domain
        self.page = page

    def write_to_database(self):
        db = MyDatabase()
        try:
            version = (
                'PostgreSQL 12.4 (Ubuntu 12.4-1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 10.2.0-5ubuntu2) 10.2.0, 64-bit',)
            # display the PostgreSQL database server version
            db_version = db.query_fetch("SELECT version()")
            # check to see if thread is already in the db
            thread_id = db.query_fetch('SELECT * FROM threads WHERE thread_id=(%s) AND title=(%s)',
                                       (self.thread_id, self.thread_title))
            if not thread_id:
                # if not, insert and get row id
                thread_id = db.perform_insert("INSERT INTO threads(thread_id, title) VALUES (%s, %s) RETURNING id",
                                              (self.thread_id, self.thread_title))
            else:
                # set row id from column data
                thread_id = thread_id[0]
            # insert link
            link_id = db.perform_insert(
                "INSERT INTO links(url, thread_id, page, domain) VALUES (%s, %s, %s, %s) RETURNING id",
                (self.link, thread_id, self.page, self.domain))
            print('Inserted thread', thread_id, 'link', link_id)
        except (Exception, psycopg2.DatabaseError) as error:
            print('DB Error: ', error)
