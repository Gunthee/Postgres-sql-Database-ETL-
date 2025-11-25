import pandas as pd
import psycopg

from ingest import ingest_data
from tranform import tranform_data, aggregate_total_amount
from publish import publish

def run_pipeline():
    #ingest 
    ingest_data()

    #tranform 
    tranform_data()
    aggregate_total_amount()

    #publish
    try: 
        publish()
        print('Data has been uploded to google sheets.')
    except:
        print('Error: fail to upload data to google sheets.')
    return

run_pipeline()