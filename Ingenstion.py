import psycopg2
import requests
import time
import logging
import pandas as pd
from google.cloud import storage
from datetime import datetime
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound


start = time.time()

#client object
storage_client = storage.Client.from_service_account_json(r"C:\Users\ThapaRa\Desktop\Pyth\helpful-graph-400611-346bbd5babdf.json")
Google_credential = service_account.Credentials.from_service_account_file(r"C:\Users\ThapaRa\Desktop\Pyth\helpful-graph-400611-346bbd5babdf.json")


#Buckets
list_bucket = list(storage_client.list_buckets())
for i in list_bucket:
    print(i)

#creating bucket
def create_bucket(bucket_name):
    storage_client = storage.Client(credentials=Google_credential)
    
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} alredy exist")

    except NotFound:
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} created")

if __name__ == '__main__':
    create_bucket(bucket_name='sonu2025')


# 1)local system

def upload_local_file(bucket_name, source_file_name, name):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination_blob_name = f"{name}_{timestamp}"

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        
        print(f"File {source_file_name} uploaded to {bucket_name} as {destination_blob_name}.")

    except Exception as e :
        print(f"Error uploading local file: {e}")


#2)psycopg2

def upload_postgreSql(hostname, database, username, pwd, port_id, table_name, gcs_bucket_name, name):
    conn = None
    curr = None

    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        curr = conn.cursor()

        query = curr.execute(f"select * from table_name")

        df = pd.read_sql(query,conn)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination_blob_name = f"{name}_{timestamp}"
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(df.to_csv(index=False))

        print("Data uploaded from PostgreSQL to {bucket_name} as {destination_blob_name}.")

    except Exception as e:
        print(f"Error uploading data from PostgreSQL: {e}")

    finally:
        if curr is not None:
            curr.close()
        if conn is not None:
            conn.close()    

#3) API

#4) FTP

#5) Sharepoint 


user_input = int(input("select upload method 1.for local file to GCS, 2. for PostgreSql to GCS ") )

if user_input == 1:
    upload_local_file('sonu2025',r'c:\Users\ThapaRa\Desktop\GCP-Test\Test_File_Dec.csv','employee_dec6')
elif user_input == 2:
    upload_postgreSql(hostname = '', 
                       database = '', 
                       username = '', 
                       pwd = '', 
                       port_id ='' , 
                       table_name = '',
                       gcs_bucket_name = '',
                       name = 'orders')
else:
    print('invalid pls select option 1 or 2')

print(time.time()-start)
