import psycopg2
from db.config import MyDatabase


def test_connection():
    # Connect to the PostgreSQL database server
    db = MyDatabase()
    print(db)
    try:
        version = (
            'PostgreSQL 12.4 (Ubuntu 12.4-1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 10.2.0-5ubuntu2) 10.2.0, 64-bit',)
        # display the PostgreSQL database server version
        db_version = db.query_fetch("SELECT version()")
        assert db_version == version, 'DB conn failed'
        print('DB connection tested')
        thread_id = db.query_fetch('SELECT * FROM "threads" WHERE thread_id=(%s) AND title=(%s)',
                                   (12, 'TEST_TITLE'))
        thread_id = thread_id[0]
        print('thread_id thru select', thread_id)
        thread_id = db.perform_insert("INSERT INTO threads(thread_id, title) VALUES (%s, %s) RETURNING id",
                                      (12, 'TEST_TITLE'))
        link_id = db.perform_insert(
            "INSERT INTO links(url, thread_id, page, domain) VALUES (%s, %s, %s, %s) RETURNING id",
            ('www.google.com/test', thread_id, 21, 'www.google.com'))
        print('Inserted thread', thread_id, 'link', link_id)
        link_deleted = db.delete_rec("DELETE FROM links WHERE id = (%s)", (link_id,))
        thread_deleted = db.delete_rec("DELETE FROM threads WHERE id = (%s)", (thread_id,))
        assert link_deleted == 1, "Link insert and deletion failed"
        assert thread_deleted == 1, "Thread insert and deletion failed"
        print('DB operations tested')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


test_connection()
