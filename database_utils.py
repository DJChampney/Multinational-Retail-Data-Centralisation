import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect


class DatabaseConnector:
    def __init__(self):
        self.db_engine = DatabaseConnector.init_db_engine

    @staticmethod
    def read_db_creds(file_path): 
        """Takes the (.yaml) file path to relevant credentials as an argument,
         reads the file and returns them as a variable"""
        with open(file_path, 'r') as f:
            try:
                credentials = yaml.safe_load(f)
                return credentials 
            except yaml.YAMLError as exc:
                print(exc)
    
    def init_db_engine(self):
        """Takes the variable output from 'read_db_creds' and initialises a
        database engine in order to connect with the Amazon RDS database"""
        credentials = self.read_db_creds('db_creds.yaml')
        database_type = credentials['TYPE']
        username = credentials['RDS_USER']
        password = credentials['RDS_PASSWORD']
        hostname = credentials['RDS_HOST']
        database = credentials['RDS_DATABASE']
        db_engine = create_engine(f"{database_type}://{username}:{password}@{hostname}/{database}")
        return db_engine

    
    def list_db_tables(self):
        """Takes the 'db_engine' output from 'init_db_engine' and returns a 
        list of available Amazon RDS tables to extract from"""        
        self.db_engine.execution_options(isolation_level='AUTOCOMMIT').connect
        inspector = inspect(self.db_engine) 
        table_names = inspector.get_table_names()
        return table_names
    
    @classmethod
    def connect_to_sql_db(cls):
        pg_creds = DatabaseConnector.read_db_creds('postgres_creds.yaml')
        database_type = pg_creds['database_type']
        database = pg_creds['database']
        user = pg_creds['user']
        password = pg_creds['password']
        host = pg_creds['host']
        port = pg_creds['port']
        cls.sql_db_engine = create_engine(f"{database_type}://{user}:{password}@{host}:{port}/{database}")
        return cls.sql_db_engine
    @staticmethod
    def upload_to_db(dataframe, table_name):
        """Takes a dataframe and desired table name as arguments, and uploads 
        the dataframe to an SQL database, defined by credentials stored in a 
        .yaml file, a message will print to confirm that the table has either
        been correctly uploaded, or that there has been an error"""

        try:
            DatabaseConnector.connect_to_sql_db()
            db_engine = DatabaseConnector.connect_to_sql_db()
            dataframe.to_sql(table_name, db_engine, if_exists='append', index=False)
            
            print(f"Data uploaded successfully to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to {table_name}: {str(e)} ")
    @staticmethod
    def upload_all(de_instance):
        print("Use default database names?")
        user_input = input('Use default database names? Y/N')
        print(f"Use default database names?: {user_input.upper()}")
        if user_input.upper() == 'N':
            values_to_be_uploaded = [attr for attr in dir(de_instance) 
                              if not attr.startswith('_') and not 
                              callable(getattr(de_instance, attr)) and 
                              attr.endswith('df')]
            to_be_uploaded = []
            for df in values_to_be_uploaded:
                to_be_uploaded.append(
                (
                str(input(f'Please provide database name for {df}')), 
                getattr(de_instance, df)
                )
                )
        
        elif user_input.upper() == 'Y':
            to_be_uploaded = [
                ('dim_users', de_instance.users_df),
                ('dim_card_details', de_instance.card_df),
                ('dim_store_details', de_instance.stores_df),
                ('dim_products', de_instance.products_df),
                ('orders_table', de_instance.orders_df),
                ('dim_date_times', de_instance.dates_df  )]
        else:
            user_input = str(input('Invalid input. Y/N: '))

        for table_name, dataframe in to_be_uploaded:
            DatabaseConnector.upload_to_db(dataframe, table_name)

    @classmethod
    def run_sql_alteration_script(cls):
        db_engine = DatabaseConnector.connect_to_sql_db()
        with open('sql_alteration_script', 'r') as file:
            sql_script = file.read()
        statements = sql_script.split(':')
        with db_engine.connect() as connection:
            for statement in statements:
                if statement.strip():
                    sql_statement = text(statement)
                    connection.execute(sql_statement)
                    connection.commit()