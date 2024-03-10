from database_utils import DatabaseConnector as dbc
import pandas as pd
import requests
import tabula
import boto3
import concurrent.futures

class DataExtractor:
    #def __init__(self):
    #    
    #    self.headers_dictionary = { "x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        
    @classmethod
    def read_rds_table(cls, table_name, dbc):
        #print(f"getting data from {table_name}")
        """Takes a 'table_name' and a DatabaseConnector instance as 
        arguments and returns the specified RDS table as a DataFrame"""
        credentials = dbc.read_db_creds('db_creds.yaml')
        rds_df = pd.read_sql_query(f'SELECT * FROM {table_name}', 
                                       dbc.init_db_engine(credentials))
        
        return rds_df
    
    @classmethod
    def retrieve_pdf_data(cls, link):
        """Takes an html link to a PDF as an argument, concatenates
        the pages and returns a dataframe"""   
        pdf_data = tabula.read_pdf(link, pages="all")
        pdf_data = pd.concat(pdf_data)
        pdf_data = pdf_data.reset_index(drop=True)
        return pdf_data
    
    def list_number_of_stores(no_of_stores_endpoint, headers_dictionary):
        """Takes an API endpoint and a headers dictionary as arguments and 
        returns the number of available stores to extract data from"""
        response = requests.get(no_of_stores_endpoint, headers=headers_dictionary)
        DataExtractor.number_of_stores = response.json()['number_stores']
        return f"{DataExtractor.number_of_stores} stores"
    
    def get_store_data(store_number):
        """Used in the 'get_data_in_chunks' method could possibly lambda this?"""
        url = f"{DataExtractor.url}/{store_number}"
        response = requests.get(url, headers=DataExtractor.headers_dictionary)
        return response.json()

    def get_data_in_chunks(store_numbers, chunk_size=4):
        """"""
        stores_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=chunk_size) as executor:
            for i in range(0, len(store_numbers), chunk_size):
                chunk = store_numbers[i:i+chunk_size]
                results = list(executor.map(DataExtractor.get_store_data, chunk))
                stores_data.extend(results)
        
        return stores_data

    
    def retrieve_stores_data(endpoint, headers):
        print("retrieving stores data from API")
        DataExtractor.url = endpoint
        DataExtractor.headers_dictionary = headers
        store_numbers = range(DataExtractor.number_of_stores) 
        stores_data = DataExtractor.get_data_in_chunks(store_numbers)
        """Takes the API endpoint and headers dictionary as arguments,
        and uses the 'number_of_stores' value from list_number_of_stores
        to retrieve the data from all API endpoints"""
        return pd.DataFrame(stores_data)
        
           
    def extract_from_s3(address):
        """Takes an Amazon S3 bucket address as an argument and converts it 
        into a DataFrame""" 
        bucket = address.split('/')[2]
        file_path = '/'.join(address.split('/')[3:])
        credentials = dbc.read_db_creds('aws_creds.yaml')
        s3 = boto3.client('s3', 
                          aws_access_key_id= credentials['access_key'],
                          aws_secret_access_key= credentials['secret_access_key'])
        s3_df = pd.read_csv(
            s3.generate_presigned_url('get_object', 
                                    Params={'Bucket': bucket, 
                                    'Key': file_path}))
        return s3_df
