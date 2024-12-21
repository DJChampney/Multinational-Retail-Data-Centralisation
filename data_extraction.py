from database_utils import DatabaseConnector as dbc
import pandas as pd
import requests
import tabula
import boto3
import concurrent.futures
import os
from tqdm import tqdm

class DataExtractor:

    dbc_instance = dbc()
    users_rds_table = 'legacy_users'
    card_data_pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    api_headers = { "x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    no_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    stores_common_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    products_s3_link = 's3://data-handling-public/products.csv'
    orders_rds_table = 'orders_table'
    dates_html = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    
    def extract_all(self):
        
        self.users_df = self.read_rds_table(
            self.users_rds_table, self.dbc_instance)
        self.card_df = self.retrieve_pdf_data(
            self.card_data_pdf_link)
        self.number_of_stores = self.list_number_of_stores(
            self.no_stores_endpoint, self.api_headers)
        self.stores_df = self.retrieve_stores_data(self.stores_common_endpoint, 
                                                self.api_headers)
        self.products_df = self.extract_from_s3(self.products_s3_link)
        self.orders_df = self.read_rds_table(self.orders_rds_table, self.dbc_instance)
        self.dates_df = self.extract_json_from_url(self.dates_html)

    
    def read_rds_table(self, table_name, dbc_instance):
        print(f"getting data from RDS table: {table_name}")
        """Takes a 'table_name' and a DatabaseConnector instance as 
        arguments and returns the specified RDS table as a DataFrame"""
        rds_df = pd.read_sql_query(f'SELECT * FROM {table_name}', 
                                       dbc_instance.init_db_engine())
        return rds_df
    
    
    def retrieve_pdf_data(self, link):
        """Takes an html link to a PDF as an argument, concatenates
        the pages and returns a dataframe"""   
        print('retrieving card_data from pdf')
        pdf_data = tabula.read_pdf(link, pages="all")
        pdf_data = pd.concat(pdf_data)
        pdf_data = pdf_data.reset_index(drop=True)
        return pdf_data
    
    
    def list_number_of_stores(self,no_of_stores_endpoint, headers_dictionary):
        """Takes an API endpoint and a headers dictionary as arguments and 
        returns the number of available stores to extract data from"""
        response = requests.get(no_of_stores_endpoint, headers=headers_dictionary)
        self.number_of_stores = int(response.json()['number_stores'])
        return self.number_of_stores
    
    
    def get_store_data(self, store_number):
        """Used in the 'get_data_in_chunks' method could possibly lambda this?"""
        url = f"{self.url}/{store_number}"
        response = requests.get(url, headers=self.headers_dictionary)
        return response.json()
        
    
    def get_data_in_chunks(self, store_numbers, chunk_size=5):
        """Submethod used by 'retrieve_stores_data', collects data from the 
        API in chunks to mitigate code runtime. If the 'message' column is created
        in the dataframe, then the chunk size is reduced"""
        while chunk_size > 0:    
            stores_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=chunk_size) as executor:
                    for i in tqdm(range(0, len(store_numbers), chunk_size)):
                        chunk = store_numbers[i:i+chunk_size]
                        results = list(executor.map(self.get_store_data, chunk))
                        stores_data.extend(results)
                        df_chunk = pd.DataFrame(results)
                        if 'message' in df_chunk.columns:
                            print(f'Missing data: trying again in blocks of {chunk_size-1}')
                            break
                    else:
                        return stores_data
            chunk_size -= 1
        print("Could not retrieve dataframe")
        return None

    
    def retrieve_stores_data(self,endpoint, headers):
        print("Retrieving stores data from API")
        self.url = endpoint
        self.headers_dictionary = headers
        store_numbers = range(self.number_of_stores) 
        stores_data = self.get_data_in_chunks(store_numbers)
        """Takes the API endpoint and headers dictionary as arguments,
        and uses the 'number_of_stores' value from list_number_of_stores
        to retrieve the data from all API endpoints"""
        return pd.DataFrame(stores_data)
        
         
    def extract_from_s3(self, address):
        """Takes an Amazon S3 bucket address as an argument and converts it 
        into a DataFrame""" 
        print('Extracting products_df from S3')
        bucket = address.split('/')[2]
        file_path = '/'.join(address.split('/')[3:])
        credentials = self.dbc_instance.read_db_creds('aws_creds.yaml')
        s3 = boto3.client('s3', 
                          aws_access_key_id= credentials['access_key'],
                          aws_secret_access_key= credentials['secret_access_key'])
        products_df = pd.read_csv(
            s3.generate_presigned_url('get_object', 
                                    Params={'Bucket': bucket, 
                                    'Key': file_path}))
        return products_df
       
    def extract_json_from_url(self, json_url):
        """Saves json data locally, stores as a df object 
        and then deletes the file"""
        response = requests.get(json_url)

        with open('dates_df.json', 'wb') as file:
            file.write(response.content)

        dates_df = pd.read_json('dates_df.json')
        os.remove('dates_df.json')
        return dates_df