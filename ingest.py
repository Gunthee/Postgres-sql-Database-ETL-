import pandas as pd
from sqlalchemy import create_engine, text 

user = 'admin'
password = '1234'
host = 'localhost'
port = '5432'
db = 'postgres'

def ingest_data():
    engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}")

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data;"))
        print("Schema 'raw_data' is created.")

    data = pd.read_csv('Retail_Transaction_Dataset.csv')

    data.to_sql(
        name = 'retail_transactions',
        con = engine,
        schema= 'raw_data',
        if_exists = 'replace',
        index = False,
        method = 'multi',
        chunksize = 1000
    )

    print('sucessfully ingest data to database')
    return 

ingest_data()