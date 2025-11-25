import pandas as pd
from sqlalchemy import create_engine
import gspread
from oauth2client.service_account import ServiceAccountCredentials

user = 'admin'
password = '1234'
host = 'localhost'
port = '5432'
db = 'postgres'


def get_df():
    engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}") #connect to the database

    # Load table
    df = pd.read_sql("SELECT * FROM production.tranformed", engine) #query tranformed data from 

    df.to_csv('tranformed2.csv')
    return

get_df()