"""
Julia Geller and Ceara Zhang
DS4300 / Twitter Relational Database
Created 14 Jan 2024
Updated: 21 Jan 2024

dbutils.py:
A collection of database utilities to make it easier
to implement a database application
"""

import mysql.connector
import pandas as pd


class DBUtils:

    def __init__(self, user, password, database, host="localhost"):
        """ Future work: Implement connection pooling """
        # Create the database
        self.create_database(user, password, host, database)
        self.con = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def close(self):
        """ Close or release a connection back to the connection pool """
        self.con.close()
        self.con = None

    def execute(self, query):
        """ Execute a select query and returns the result as a dataframe """

        # Step 1: Create cursor
        rs = self.con.cursor()

        # Step 2: Execute the query
        rs.execute(query)

        # Step 3: Get the resulting rows and column names
        rows = rs.fetchall()
        cols = list(rs.column_names)

        # Step 4: Close the cursor
        rs.close()

        # Step 5: Return result
        return pd.DataFrame(rows, columns=cols)

    def insert_one(self, sql, val):
        """ Insert a single row """
        cursor = self.con.cursor()
        cursor.execute(sql, val)
        self.con.commit()

    def insert_many(self, sql, vals):
        """ Insert multiple rows """
        cursor = self.con.cursor()
        cursor.executemany(sql, vals)
        self.con.commit()

    def create_database(self, user, password, host, database):
        """ Insert multiple rows """
        # create connection
        con = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
        )

        # create cursor
        cursor = con.cursor()

        # create the database
        cursor.execute(f"CREATE DATABASE {database}")

        # close the cursor and connection
        cursor.close()
        con.close()



