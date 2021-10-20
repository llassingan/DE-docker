#import library
import psycopg2
#initiate sql command
#username must be unique
syntax = [["""
            CREATE TABLE accounts (
            username VARCHAR ( 50 ) UNIQUE NOT NULL,
            info VARCHAR ( 50 ) NOT NULL 
            );
        ""","""
        INSERT INTO accounts(username, info) VALUES ('lingga','admin');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('inggal','user');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('nggali','super');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('ggalin','leader');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('galing','regular');
        """],
         ["""
            CREATE TABLE accounts (
            username VARCHAR ( 50 ) UNIQUE NOT NULL,
            info VARCHAR ( 50 ) NOT NULL 
            );
        """],["""
        INSERT INTO accounts(username, info) VALUES ('almanik','admin');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('kalmani','admin');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('ikalman','user');
        """],["""
        INSERT INTO accounts(username, info) VALUES ('nikalma','admin');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('anikalm','admin');
        ""","""
        INSERT INTO accounts(username, info) VALUES ('manikal','user');
        """,]]

def dbinit(h,p,d,u,ps,com):
    host = h
    port = p
    database = d
    user = u
    password = ps
    
    conn = None
    try:
        conn = psycopg2.connect(host=host,
                                port=port,
                                database=database,
                                user=user,
                                password=password)
        cur = conn.cursor()
        commands = com
        for i in range(len(commands)):
            cur.execute(commands[i])
            print("executing command no:",i)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#initiate database1
dbinit("localhost","5961","airflow","airflow","airflow",syntax[0])
#initiate database2
dbinit("localhost","5105","airflow2","airflow2","airflow2",syntax[1])

#add more values to database1
# dbinit("localhost","5961","airflow","airflow","airflow",syntax[2])

#add more values to database1
# dbinit("localhost","5961","airflow","airflow","airflow",syntax[3])