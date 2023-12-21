import snowflake.connector
import pandas as pd

# Connecting with snowflake

cnn = snowflake.connector.connect(
    user="LUCKOSTER",
    password="*************",
    account="EPORRXE-WU15947"
    )
cs = cnn.cursor()

# Creating a warehouse, database, schema and table using SQL inserting test rows
try:
    cs.execute("select current_version()")
    row = cs.fetchone()
    print(row[0])
    print("creating warehouse..")
    sql = "CREATE WAREHOUSE IF NOT EXISTS project_warehouse"
    cs.execute(sql)
    print("creating database..")
    sql = "CREATE DATABASE IF NOT EXISTS project_database"
    cs.execute(sql)
    print("using database..") 
    sql = "USE DATABASE project_database"
    cs.execute(sql)
    print("creating schema..")
    sql = "CREATE SCHEMA IF NOT EXISTS project_schema"
    cs.execute(sql)
    print("creation complete..")
    sql = "USE WAREHOUSE project_warehouse"
    cs.execute(sql)
    sql = "USE DATABASE project_database"
    cs.execute(sql)
    sql = "USE SCHEMA project_schema"
    cs.execute(sql)
    print("create a table..")
    sql = ("CREATE OR REPLACE TABLE project_comments"
           " (ID integer, comments string)")
    cs.execute(sql)
    print("insert a few rows..")
    sql = ("insert into project_comments (ID, comments)"
           " values (1, 'my comments about the project!')")
    cs.execute(sql)
    sql = ("Insert into project_comments (ID, comments)"
           " Values (2, 'some more comments about the project!')")
    cs.execute(sql)
    sql = ("Insert into project_comments (ID, comments)"
           " Values (3, 'even more comments about the project!')")
    cs.execute(sql)
    print("read some rows..")
    sql = "select * from project_comments"
    cs.execute(sql)
    for row in cs.fetchall():
           print(row)
    print("complete.")
    
finally:
    cs.close()
cnn.close()