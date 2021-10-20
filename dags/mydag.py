# import library
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

# function for query data from the accounts table on first database
def _get_data():
    """ query data from the accounts table on first database"""
    conn = None
    try:
        #connect to both of the db
        conn = psycopg2.connect(host="localhost",
                                port="5961",
                                database="airflow",
                                user="airflow",
                                password="airflow")
        conn2 = psycopg2.connect(host="localhost",
                                port="5105",
                                database="airflow2",
                                user="airflow2",
                                password="airflow2")
        # cursor
        cur = conn.cursor()
        cur2 = conn2.cursor()
        # query
        cur.execute("SELECT * FROM accounts")
        cur2.execute("SELECT * FROM accounts")
        #fetch
        row = cur.fetchall()
        row2 = cur2.fetchall()
        # check the last data
        #if the second db is not empty
        if len(row2) != 0:
            #data from the first db will be loaded to the second db starting from the "last-data-of-2nd-db" + 1
            # db1 = 1,2,3,4
            # db2 = 1,2
            # new data that will be transfered from db 1 to db 2 are "3" and "4"
            dataset = row[row.index(row2[-1])+1:]
        # if the 2nd db is empty:
        elif len(row2) == 0:
            # transfer all the data from db1
            dataset = row
        cur.close()
        return dataset   
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# loading data from the first database and insert into second database
def _load_data(ti):
    """ loading data from the first database and insert into second database"""
    conn = None
    try:
        conn = psycopg2.connect(host="localhost",
                                port="5105",
                                database="airflow2",
                                user="airflow2",
                                password="airflow2")
        cur = conn.cursor()
        # get the data from the first task
        commands = ti.xcom_pull(task_ids="get_data")
        print(commands)
        # load all the data into 2nd db
        for i in range(len(commands)):
            query = "INSERT INTO accounts(username, info) VALUES ('"+ str(commands[i][0])+"','"+str(commands[i][1])+"');"
            cur.execute(query)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# dag
with DAG("my_dag",start_date=datetime(2021,10,20), schedule_interval="@daily", catchup=False) as dag:
    # 1st task
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data
    )
    # 2nd task
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=_load_data
    )
    # dag flow
    get_data >> load_data