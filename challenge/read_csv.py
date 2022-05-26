# import libs
import os
from dotenv import load_dotenv
import pandas as pd
import datetime
from sqlalchemy import create_engine
import shutil
import glob

# get env
load_dotenv()

#load variables
landingpath = os.getenv("LANDINGPATH")
processedpath = os.getenv("PROCESSEDPATH")
mssql = os.getenv("MSSQL")



# read files from landing zone [one or more files]
csv_files = glob.glob(os.path.join(landingpath, "*.csv"))
df_from_each_file = (pd.read_csv(f) for f in csv_files)
read_csv = pd.concat(df_from_each_file, ignore_index=True)
print('loading csv files...')

# add new column for timestamp
trips = read_csv.assign(dt_timestamp=datetime.datetime.now())
print('adding timestamp ingestion column...')

# insert data into SQL Server
mssql_engine = create_engine(mssql)
trips.to_sql('trips', mssql_engine, if_exists='append', index=False, chunksize=10)
print('insert into SQL Server database...')

# in the end of the process move the landing file to processed zone
file_names = os.listdir(landingpath)
for file_name in file_names:
    shutil.move(os.path.join(landingpath, file_name), processedpath)
print('moving landing files to processed zone...')