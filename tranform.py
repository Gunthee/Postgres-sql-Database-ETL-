import pandas as pd
from sqlalchemy import create_engine

import pandas as pd
from sqlalchemy import create_engine, text 

user = 'admin'
password = '1234'
host = 'localhost'
port = '5432'
db = 'postgres'

engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}")


def tranform_data():

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS production;"))
        print("Schema 'production' is created.")

    df = pd.read_sql("SELECT * FROM raw_data.retail_transactions", engine)

    # Convert to datetime (format = MM/DD/YYYY HH:MM)
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], format="%m/%d/%Y %H:%M")

    # Create new formatted columns
    df["Date"] = df["TransactionDate"].dt.strftime("%m/%d/%Y")   # mm/dd/yy
    df["Time"] = df["TransactionDate"].dt.strftime("%H:%M")       # hh:mm

    # Drop original column
    df = df.drop(columns=["TransactionDate"])

    df["Postcode"] = df["StoreLocation"].str.extract(r'(\d{5})$')  #tranform location
    df = df.drop(columns=["StoreLocation"]) # drop the original column 

    #calculate the amount without discount 
    df['Amount'] = df['Price']*df['Quantity']

    #calculate price diff
    df['Price_diff'] = df["Amount"] - df["TotalAmount"] 


    print(df.head())

    df.to_sql(
    name = 'tranformed',
    con= engine,
    schema= 'production',
    if_exists= 'replace',
    index= False,
    method = 'multi',
    chunksize= 1000
    )

    print('sucessfully insert to production')
    return

def aggregate_total_amount():

    sql = '''
        SELECT 
            TO_CHAR(TO_DATE("Date", 'DD/MM/YYYY'), 'YYYY-MM') AS Month,
            SUM("TotalAmount") AS TotalAmount_By_Month
        FROM 
            production.tranformed
        GROUP BY 
            TO_CHAR(TO_DATE("Date", 'DD/MM/YYYY'), 'YYYY-MM')
        ORDER BY 
            TO_CHAR(TO_DATE("Date", 'DD/MM/YYYY'), 'YYYY-MM');
        '''

    df = pd.read_sql(sql,engine)

    print(df)

    df.to_sql(
        name = 'aggregate_TotalAmount',
        con= engine,
        schema= 'production',
        if_exists= 'replace',
        index= False,
        method = 'multi',
        chunksize= 1000
    )
    print('sucessfully insert to production')

    return

tranform_data()



