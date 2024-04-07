import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect


class DatabaseConnector:
    @staticmethod
    def ask_for_credentials():
        """Calls all methods that ask the user for credentials, creating the 
        relevant YAML files in the directory"""
        DatabaseConnector.ask_for_db_creds()
        DatabaseConnector.ask_for_postgres_creds()
        DatabaseConnector.ask_for_aws_creds()

    @staticmethod
    def ask_for_db_creds():
        """When called, asks the user for RDS credentials which will be stored
        in a YAML file in the working directory."""

        default_db_and_port = input("RDS Database: postgres & RDS port: 5432?(Y/N): ")

        if default_db_and_port.upper() == "Y":
            rds_database = "postgres"
            rds_port = 5432
        elif default_db_and_port.upper() == "N": 
            rds_database = f"{input("Please enter RDS database(eg. 'postgres'): ")}"
            rds_port = f"{input("Please enter RDS port(eg. '5432'): ")}"
        else:
            input(f"Did not recognize {default_db_and_port}, RDS Database: postgres & RDS port: 5432?(Y/N) ")           

        rds_creds = {"TYPE": "postgresql",
            "RDS_HOST": input('Please enter RDS host: '),
            "RDS_USER": input('Please enter RDS username: '),
            "RDS_PASSWORD": input('Please enter RDS password: ' ),
            "RDS_DATABASE": rds_database,
            "RDS_PORT": rds_port}
        file_name = "db_creds.yaml"
        with open(file_name, "w") as yaml_file:
            yaml.dump(rds_creds, yaml_file)

    @staticmethod
    def ask_for_postgres_creds():
        default_db_and_port = input("RDS User: postgres, RDS Database: postgresql, RDS host: localhost & RDS port: 5432?(Y/N): : ")

        if default_db_and_port.upper() == "Y":
            user = "postgres"
            database = "postgresql"
            host = "localhost"
            port = 5432
        elif default_db_and_port.upper() == "N": 
            database = f"{input("Please enter RDS database(eg. 'postgresql'): ")}"
            host = input("Please enter postgres host: ")
            port = f"{input("Please enter RDS port(eg. '5432'): ")}"
        else:
            input(f"Did not recognize {default_db_and_port}, RDS Database: postgresql & RDS port: 5432?(Y/N) ")           

        postgres_creds = {"database_type": database,
        "database": "sales_data",
        "user": user,
        "password": input(f"Please enter {database} password: "),
        "host": host,
        "port": port}
            
        file_name = "postgres_creds.yaml"
        with open(file_name, "w") as yaml_file:
            yaml.dump(postgres_creds, yaml_file)

    @staticmethod
    def ask_for_aws_creds():
        aws_creds = {"access_key": input('Please enter AWS access key: '),
            "secret_access_key": input('Please enter AWS secret access key: ')}
            
        file_name = "aws_creds.yaml"
        with open(file_name, "w") as yaml_file:
            yaml.dump(aws_creds, yaml_file)

    @classmethod
    def read_db_creds(cls, file_path): 
        """Takes the (.yaml) file path to relevant credentials as an argument,
         reads the file and returns them as a dictionary"""
        with open(file_path, "r") as f:
            try:
                cls.rds_credentials = yaml.safe_load(f)
                return cls.rds_credentials 
            except yaml.YAMLError as exc:
                print(exc)

    @classmethod
    def init_db_engine(cls):
        """Takes the dictionary output from 'read_db_creds' and initialises a
        database engine in order to connect with the Amazon RDS database"""
        credentials = cls.read_db_creds('db_creds.yaml')
        database_type = cls.rds_credentials['TYPE']
        username = cls.rds_credentials['RDS_USER']
        password = cls.rds_credentials['RDS_PASSWORD']
        hostname = cls.rds_credentials['RDS_HOST']
        database = cls.rds_credentials['RDS_DATABASE']
        db_engine = create_engine(f"{database_type}://{username}:{password}@{hostname}/{database}")
        return db_engine

    @classmethod
    def list_db_tables(cls):
        """Takes the 'db_engine' output from 'init_db_engine' and returns a 
        list of available Amazon RDS tables to extract from"""        
        cls.db_engine.execution_options(isolation_level='AUTOCOMMIT').connect
        inspector = inspect(cls.db_engine) 
        table_names = inspector.get_table_names()
        return table_names
    
    @classmethod
    def connect_to_sql_db(cls):
        """Called within the upload_to_db() method, takes the provided 
         PostgreSQL credentials and creates an engine to connect to the
         relevant SQL database"""
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
        """Takes an instance of DataExtractor as an argument and asks for user
         input(Y/N) to determine whether to use default(Y) or custom(N) table 
         names when uploading to the database. CAUTION: If using custom table
         names, the 'run_sql_alteration_script()' method will not work correctly
         unless the attached 'sql_alteration_script' file is edited to match"""
        user_input = input('Use default table names? Y/N')
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
        """Runs an SQL query stored in the working directory, which formats
        the column types of the SQL database and creates the star-based schema
        """
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
                    print("sales_data successfully formatted")
    

