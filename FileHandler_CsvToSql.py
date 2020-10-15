from numpy.lib.utils import source
import pyodbc
import os
import csv
import pandas as pd
import glob
from pandas.io import sql
import sqlalchemy as sa
import urllib
import shutil

from mycredentials import my_connection

def main():
    print("-main-")
    print("--file_handler--")
    file_handler()

def file_handler():

    # Connection to sql DB
    server = my_connection.server 
    database = my_connection.database 
    username = my_connection.username 
    password = my_connection.password 
    
    connection_str = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connection_str))

    path = '/mnt/scripts/filehandler/'
    all_files = glob.glob(os.path.join(path, "*.txt"))    

    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0, encoding = 'utf-8-sig')
        li.append(df)
    
    frame = pd.concat(li, axis=0, ignore_index=True)

    frame['t_item'] = ""
    frame['t_side'] = ""
    frame['t_revi'] = ""
    frame['t_mach'] = ""

    for index, row in frame.iterrows():

        if "SK" in row.g_board_name:

            if row.g_board_name[7] == "L":
                row.t_item = row.g_board_name[:7]
                row.t_side = row.g_board_name[7:9]
                row.t_revi = row.g_board_name[9:-4]
                row.t_mach = row.g_board_name[-3:]

            elif row.g_board_name[8] == "L":
                row.t_item = row.g_board_name[:8]
                row.t_side = row.g_board_name[8:10]
                row.t_revi = row.g_board_name[10:-4]
                row.t_mach = row.g_board_name[-3:]

            else:
                row.t_item = ""
                row.t_side = ""
                row.t_revi = ""
                row.t_mach = ""

    frame.to_sql('mytable', engine, schema='dbo', if_exists='append', index=False, index_label='t_item')

    file_mover()


def file_mover():

    print("---FileMover---")

    source = '/mnt/scripts/filehandler/'
    destination = '/mnt/scripts/filehandler/processed/'

    all_files = glob.glob(os.path.join(source, "*.txt"))    

    for filename in all_files:
        shutil.move(filename, destination)    

    print("---FileMover---")

if __name__ == "__main__":
    main()
