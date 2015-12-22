# -*- coding: utf-8 -*-
"""
Created on Sun Dec 20 21:33:18 2015

@author: Administrator
"""

import requests
import datetime
import sqlite3 as lite
import collections
import time
from pandas.io.json import json_normalize
from dateutil.parser import parse 

r = requests.get("http://www.citibikenyc.com/stations/json")
df = json_normalize(r.json()['stationBeanList'])

con = lite.connect('citi_bike.db')
cur = con.cursor()

# Drop tables if they already exist
with con:
    cur.execute("DROP TABLE IF EXISTS citibike_reference")
    cur.execute("DROP TABLE IF EXISTS available_bikes")

# Create reference table
with con:
    cur.execute('CREATE TABLE citibike_reference ( '
        'id INT PRIMARY KEY,'
        'totalDocks INT,'
        'city TEXT,'
        'altitude INT,'
        'stAddress2 TEXT,'
        'longitude NUMERIC,'
        'postalCode TEXT,'
        'testStation TEXT,'
        'stAddress1 TEXT,'
        'stationName TEXT,'
        'landMark TEXT,'
        'latitude NUMERIC,'
        'location TEXT )')

#a prepared SQL statement we're going to execute over and over again
sql = ("INSERT INTO citibike_reference ( "
    "id, totalDocks, city, altitude, stAddress2, "
    "longitude, postalCode, testStation, stAddress1, "
    "stationName, landMark, latitude, location) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        vals = (
            station['id'],
            station['totalDocks'],
            station['city'],
            station['altitude'],
            station['stAddress2'],
            station['longitude'],
            station['postalCode'],
            station['testStation'],
            station['stAddress1'],
            station['stationName'],
            station['landMark'],
            station['latitude'],
            station['location']
        )
        cur.execute(sql,vals)

#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the available bikes table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
with con:   
    cur.execute("CREATE TABLE available_bikes ( "
        "execution_time INT, " +  ", ".join(station_ids) + ");")

def update_available_bikes():
    r = requests.get("http://www.citibikenyc.com/stations/json")

    #take the string and parse it into a Python datetime object
    exec_time = parse(r.json()['executionTime'])

    exec_time = (exec_time - datetime.datetime(1970,1,1)).total_seconds()

    with con:
        cur.execute('INSERT INTO available_bikes ( '
            'execution_time) VALUES (?)', (exec_time,))

    id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station

    #loop through the stations in the station list
    for station in r.json()['stationBeanList']:
        id_bikes[station['id']] = station['availableBikes']

    #iterate through the defaultdict to update the values in the database
    with con:
        for k, v in id_bikes.iteritems():
            cur.execute("UPDATE available_bikes SET _" + 
                str(k) + " = " + str(v) + " WHERE execution_time = " + 
                    str(exec_time) + ";")
                    
count = 0
while count < 61:
    update_available_bikes()
    count += 1
    time.sleep(60)                    