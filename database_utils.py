import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect
import psycopg2
import pandas as pd
import sqlite3


class DatabaseConnector:
    
    def read_db_creds(file_path): 
        """Takes the (.yaml) file path to relevant credentials as an argument,
         reads the file and returns them as a variable"""
        with open(file_path, 'r') as f:
            try:
                credentials = yaml.safe_load(f)

                return credentials 
            except yaml.YAMLError as exc:
                print(exc)
    
    def init_db_engine(credentials):
        """Takes the variable output from 'read_db_creds' and initialises a
        database engine in order to connect with the Amazon RDS database"""
        database_type = credentials['TYPE']
        username = credentials['RDS_USER']
        password = credentials['RDS_PASSWORD']
        hostname = credentials['RDS_HOST']
        database = credentials['RDS_DATABASE']
        db_engine = create_engine(f"{database_type}://{username}:{password}@{hostname}/{database}")
        return db_engine
        
  
    def list_db_tables(db_engine):
        """Takes the 'db_engine' output from 'init_db_engine' and returns a 
        list of available Amazon RDS tables to extract from"""        
        db_engine.execution_options(isolation_level='AUTOCOMMIT').connect
        inspector = inspect(db_engine)
        table_names = inspector.get_table_names()
        return table_names
        

            
    def upload_to_db(dataframe, table_name):
        """Takes a dataframe and desired table name as arguments, and uploads 
        the dataframe to an SQL database, defined by credentials stored in a 
        .yaml file, a message will print to confirm that the table has either
        been correctly uploaded, or that there has been an error"""
        pg_creds = DatabaseConnector.read_db_creds('postgres_creds.yaml')
        database_type = pg_creds['database_type']
        database = pg_creds['database']
        user = pg_creds['user']
        password = pg_creds['password']
        host = pg_creds['host']
        port = pg_creds['port']
        try:
            db_engine = create_engine(
                f"{database_type}://{user}:{password}@{host}:{port}/{database}")
            dataframe.to_sql(table_name, db_engine, if_exists='append', index=False)
            
            print(f"Data uploaded successfully to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to {table_name}: {str(e)} ")
        
            