### Ingest CSV file using Python

This code was create to ingest data from a csv file inside SQL Server database using python coding.

for this we use the follow main libs:

* Pandas
* SQLAlchemy

Let's explain how the code works.

Most important thing is that all the libs that we need are inside the file requirements.txt

Install all using this file.

This first part is where we are going to import the libs:

```py
import os
from dotenv import load_dotenv
import pandas as pd
import datetime
from sqlalchemy import create_engine
import shutil
import glob

```

After this we are going to load the variables, we are using .env file where the files path are and database connection string:

```py
# get env
load_dotenv()

#load variables
landingpath = os.getenv("LANDINGPATH")
processedpath = os.getenv("PROCESSEDPATH")
mssql = os.getenv("MSSQL")
```
Now we are going to start the real code part, first is to read the file, this block was made to read one or multiple CSV files.
And we add a new column as our ingestion timestamp:
```py
# read files from landing zone [one or more files]
csv_files = glob.glob(os.path.join(landingpath, "*.csv"))
df_from_each_file = (pd.read_csv(f) for f in csv_files)
read_csv = pd.concat(df_from_each_file, ignore_index=True)
print('loading csv files...')

# add new column for timestamp
trips = read_csv.assign(dt_timestamp=datetime.datetime.now())
print('adding timestamp ingestion column...')

```

With the file load as dataframe in pandas, we are going to insert into database using SQLAlchemy:
```py
# insert data into SQL Server
mssql_engine = create_engine(mssql)
trips.to_sql('trips', mssql_engine, if_exists='append', index=False, chunksize=10)
print('insert into SQL Server database...')
```

To conclude our ingestion we going to move the file from landing zone to processed zone.
This is a good pattern when we think that going to be multiple files been ingested all the time.

```py
# in the end of the process move the landing file to processed zone
file_names = os.listdir(landingpath)
for file_name in file_names:
    shutil.move(os.path.join(landingpath, file_name), processedpath)
print('moving landing files to processed zone...')
```