from numpy.lib.function_base import append
import pyodbc
import os
import csv
import pandas as pd
import glob
from pandas.io import sql
import sqlalchemy as sa
import urllib

from sqlalchemy import schema
from dbfread import DBF

from mycredentials import my_connection

var = '/my_path/L0013107.DBF'

def main():
    print("-main-")
    print("--DBF to CSV--")
    dbf_to_csv(var)
    print("---FileHandler---")
    file_handler()
    print("-main-")

def dbf_to_csv(dbf_table_pth):
    
    # Set csv file name removing ".DBF" extension
    csv_filename = dbf_table_pth[:-4]+ ".csv" 
    
    # Define table variable as DBF object
    table = DBF(dbf_table_pth)

    # Create & write .csv file
    with open(csv_filename, 'w', newline = '') as f:
        writer = csv.writer(f)
        
        # Write field name
        writer.writerow(table.field_names)
        
        # Write rows
        for record in table:
            writer.writerow(list(record.values()))
    
    # Return csv name
    #return csv_filename

def file_handler():

    # Connection to sql DB
    server = my_connection.server 
    database = my_connection.database 
    username = my_connection.username 
    password = my_connection.password 

    connection_str = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connection_str))

    path = '/my_path/'
    all_files = glob.glob(os.path.join(path, "L*.csv"))    

    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0, encoding = 'utf-8-sig')
        li.append(df)
    
    frame = pd.concat(li, axis=0, ignore_index=True)

    # Business logic
    frame['DATUM'] = frame['DATUM'].str.replace("-", "")

    frame.to_sql("mytable", engine, schema='dbo', if_exists='append', index=False, index_label='DATUM')


if __name__ == "__main__":
    main()
