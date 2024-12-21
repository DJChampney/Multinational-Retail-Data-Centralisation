import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect


class DatabaseConnector:

    def __init__(self):
        self.rds_credentials = self.read_db_creds('db_creds.yaml')
        self.pg_creds = self.read_db_creds('postgres_creds.yaml')
        self.sql_db_engine = self.connect_to_sql_db()


    def read_db_creds(self, file_path): 
        """Takes the (.yaml) file path to relevant credentials as an argument,
         reads the file and returns them as a dictionary"""
        with open(file_path, "r") as f:
            try:
                credentials = yaml.safe_load(f)
                return credentials 
            except yaml.YAMLError as exc:
                print(exc)

    
    def init_db_engine(self):
        """Takes the dictionary output from 'read_db_creds' and initialises a
        database engine in order to connect with the Amazon RDS database"""
        credentials = self.read_db_creds('db_creds.yaml')
        database_type = self.rds_credentials['TYPE']
        username = self.rds_credentials['RDS_USER']
        password = self.rds_credentials['RDS_PASSWORD']
        hostname = self.rds_credentials['RDS_HOST']
        database = self.rds_credentials['RDS_DATABASE']
        db_engine = create_engine(f"{database_type}://{username}:{password}@{hostname}/{database}")
        return db_engine

    
    def list_db_tables(self):
        """Takes the 'db_engine' output from 'init_db_engine' and returns a 
        list of available Amazon RDS tables to extract from"""        
        self.db_engine.execution_options(isolation_level='AUTOCOMMIT').connect
        inspector = inspect(self.db_engine) 
        table_names = inspector.get_table_names()
        return table_names
    
    
    def connect_to_sql_db(self):
        """Called within the upload_to_db() method, takes the provided 
         PostgreSQL credentials and creates an engine to connect to the
         relevant SQL database"""
        database_type = self.pg_creds['database_type']
        database = self.pg_creds['database']
        user = self.pg_creds['user']
        password = self.pg_creds['password']
        host = self.pg_creds['host']
        port = self.pg_creds['port']
        sql_db_engine = create_engine(f"{database_type}://{user}:{password}@{host}:{port}/{database}")
        return sql_db_engine
    
    
    def upload_to_db(self, dataframe, table_name):
        """Takes a dataframe and desired table name as arguments, and uploads 
        the dataframe to an SQL database, defined by credentials stored in a 
        .yaml file, a message will print to confirm that the table has either
        been correctly uploaded, or that there has been an error"""

        try:
            self.connect_to_sql_db()
            dataframe.to_sql(table_name, self.sql_db_engine, if_exists='append', index=False)
            
            print(f"Data uploaded successfully to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to {table_name}: {str(e)} ")

    def upload_all(self, de_instance):
        """Takes an instance of DataExtractor as an argument and asks for user
         input(Y/N) to determine whether to use default(Y) or custom(N) table 
         names when uploading to the database. CAUTION: If using custom table
         names, the 'run_sql_alteration_script()' method will not work correctly
         unless the attached 'sql_alteration_script' file is edited to match"""

        to_be_uploaded = [
            ('dim_users', de_instance.users_df),
            ('dim_card_details', de_instance.card_df),
            ('dim_store_details', de_instance.stores_df),
            ('dim_products', de_instance.products_df),
            ('orders_table', de_instance.orders_df),
            ('dim_date_times', de_instance.dates_df  )]

        for table_name, dataframe in to_be_uploaded:
            self.upload_to_db(dataframe, table_name)

    
    def run_sql_alteration_script(self):
        """Runs an SQL query stored in the working directory, which formats
        the column types of the SQL database and creates the star-based schema
        """
        
        with open('sql_alteration_script', 'r') as file:
            sql_script = file.read()
        statements = sql_script.split(':')
        with self.sql_db_engine.connect() as connection:
            for statement in statements:
                if statement.strip():
                    sql_statement = text(statement)
                    connection.execute(sql_statement)
                    connection.commit()
                    print("sales_data successfully formatted")
    









