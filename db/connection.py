# Connecting to SQL DB for handling server configurations
import mysql.connector
import os
from dotenv import load_dotenv
import logging
from typing import List
import time

# in case it hasn't already been done yet
load_dotenv()

class Connection:
    _info = {"host": os.environ["DBHOST"],
        "user": os.environ["DBUSER"],
        "password": os.environ["DBPASS"],
        "database":os.environ["DBNAME"]}
    last_connected = time.time()
    connection_live = 120 # 2 minute(s) per connection
    conn = mysql.connector.connect(**_info)

    @classmethod
    def execute(cls, query: str):
        if time.time() - cls.last_connected > cls.connection_live:
            cls.conn.close()
            cls.conn = mysql.connector.connect(**cls._info)
            cls.last_connected = time.time()
        while not cls.conn.is_connected():
            logging.info("DB disconnected. Now reconnecting")
            cls.conn = mysql.connector.connect(**cls._info)
            cls.last_connected = time.time()
        cursor = cls.conn.cursor()
        cursor.execute(query)
        res = list(cursor.fetchall())
        cls.conn.commit()
        cursor.close()
        return res
    
    @classmethod 
    def exec_all(cls, queries: List[str]):
        if time.time() - cls.last_connected > cls.connection_live:
            cls.conn.close()
            cls.conn = mysql.connector.connect(**cls._info)
            cls.last_connected = time.time()
        while not cls.conn.is_connected():
            logging.info("DB disconnected. Now reconnecting")
            cls.conn = mysql.connector.connect(**cls._info)
            cls.last_connected = time.time()
        cursor = cls.conn.cursor()
        for q in queries:
            cursor.execute(q)
            cursor.fetchall()
        cls.conn.commit()
        cursor.close()
