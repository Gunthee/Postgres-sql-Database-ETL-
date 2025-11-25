import pandas as pd
from sqlalchemy import create_engine
import gspread
from oauth2client.service_account import ServiceAccountCredentials

user = 'admin'
password = '1234'
host = 'localhost'
port = '5432'
db = 'postgres'


def get_df()->pd.DataFrame:
    engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}") #connect to the database

    # Load table
    df = pd.read_sql("SELECT * FROM production.tranformed", engine) #query tranformed data from 

    print(df.shape)
    return df 


def publish():

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
            ]

    credentials = ServiceAccountCredentials.from_json_keyfile_name('bigdata-479203-e98a81f56534.json', scope)

    client = gspread.authorize(credentials=credentials)

    sheet = client.create("BigData")

    sheet.share('gunthee.kanj@bumail.net', perm_type='user', role= 'writer')

    sheet = client.open('BigData')

    df = get_df()

    df.columns.values.tolist() #get column name

    sheet.update([df.columns.values.tolist()]+ [df.values.tolist()]) #insert column name and data to google sheet
    print('Data has been uploaded to google sheets.')

    return 


   










