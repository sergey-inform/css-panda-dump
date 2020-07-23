#!/usr/bin/env python3
''' Connect to Slow Control database. 
    Update data in csv-files with most recent data from DB.
'''


import os
import configparser

import psycopg2
import logging

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)

# 120 | DINOMAGN:AM3202:Humid      
# 121 | DINOMAGN:AM3202:Temp       
# 122 | DINOMAGN:BME280:Humid   
# 123 | DINOMAGN:BME280:Temp   
# 168 | DINOMAGN:BME280:Pres


conf = { 
        'database': {
            'host': 'localhost',
            'port': '5432',
            'dbname': 'archive',
            'user': 'archive',
            'password': 'secret',  # no, it's not ;)
            },
        'channels': {
            "DINOMAGN:AM3202:Humid" : "HumA",
            "DINOMAGN:AM3202:Temp"  : "TempA", 
            "DINOMAGN:BME280:Humid" : "HumB",
            "DINOMAGN:BME280:Temp"  : "TempB", 
            "DINOMAGN:BME280:Pres"  : "PresB",
            },
        }

# Read config file
with open("./config.ini", 'r') as f:
    parser = configparser.ConfigParser()
    parser.read_file(f)
    for section in parser.sections():
        if section in conf:
            # Replace default
            conf[section] = dict(parser[section])
        
#print(conf)

try:
    conn = psycopg2.connect(**conf['database'])
except:
    print("Unable to connect to the database.")
    exit(1)

cur = conn.cursor()

# Get channel IDs
names = tuple(conf['channels'].keys()) 
query = cur.mogrify("""SELECT channel_id, name from channel where name in %s""",
                        (names, ) )
try:
    cur.execute(query)
except:
    print(query) 
    print("can't get tables")
    exit(1)

rows = cur.fetchall()

id_map = [(conf['channels'][name], id) for id, name in rows]
print(id_map)

FIRST_TS = 1594796513.96

for name, channel_id in id_map:
    query = cur.mogrify("SELECT extract(epoch from smpl_time), float_val "
                            "from sample where channel_id = %s "
                            "and smpl_time > to_timestamp(%s) at time zone 'Europe/Moscow' "
                            "order by smpl_time asc",
                                    (channel_id, FIRST_TS) )
    try:
        cur.execute(query)
    except: 
        print(query)
        print("can't execute a query")
        exit(1)
   
    print(name)

    with open(name.lower()+'.log', 'w+') as file:
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for r in rows:
                file.write("{};{}\n".format(*r))

#TODO: get last timestamp in csv files.


conn.close()

