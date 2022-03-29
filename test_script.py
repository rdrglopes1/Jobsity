#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

import psycopg2

import os

import psycopg2.extras

connection = psycopg2.connect(
    host="localhost",
    database="testDB",
    user="postgres",
    password=None,
)
connection.autocommit = True

url='https://drive.google.com/file/d/14JcOSJAWqKOUNyadVZDPm7FplA7XYhrU/view'
url='https://drive.google.com/uc?id=' + url.split('/')[-2]

def load_db():
    df = pd.read_csv(url)
    
    def copy_from_file(conn, df, table):
        """
        Here we are going save the dataframe on disk as 
        a csv file, load the csv file  
        and use copy_from() to copy it to the table
        """

        tmp_df = "./tmp_dataframe.csv"
        df.to_csv(tmp_df, index=False, header=False)
        f = open(tmp_df, 'r')
        cursor = conn.cursor()
        trunc_query = "TRUNCATE TABLE %s"% (table)
        if len(df) > 0:
            cursor.execute(trunc_query)
        try:
            cursor.copy_from(f, table, sep=",")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            os.remove(tmp_df)
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("database loaded")
        cursor.close()
    copy_from_file(connection, df, 'trips')

reload = input('would you like to reload database(y/n)?')

if reload == 'y':
    load_db()

# option = imput('choose trips information to display')

def select_data(query):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    select_Query = query
    cursor.execute(select_Query)
    result = cursor.fetchall()
    dict1 = []
    for row in result:
        dict1.append(dict(row))
    
    return dict1

all_trips = select_data("SELECT * FROM trips")

weekly_avg_by_region = select_data("select region, TO_CHAR(datetime, 'Mon') as Month, (COUNT(*)/MAX(TO_CHAR(datetime, 'W')::NUMERIC)) as week_avg FROM public.trips group by region, TO_CHAR(datetime, 'Mon')")

status_loaded = select_data("SELECT COUNT(*)as volume_loaded, MAX(datetime) as last_datetime FROM trips")

option = input('choose trips information to display: \n 1 - all_trips \n 2 - weekly avg by region \n 3 - status loaded \n')

if option == '1':
    print(all_trips)
elif option == '2':
    print(weekly_avg_by_region)
elif option == '3':
    print(status_loaded)
else:
    print('choose an available option, please type 1, 2 or 3')





