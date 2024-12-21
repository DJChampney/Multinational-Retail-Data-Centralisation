from database_utils import DatabaseConnector as dbc
from data_extraction import DataExtractor
from data_cleaning import DataCleaning as dc
dbc_instance = dbc()
de_instance = DataExtractor()  
de_instance.extract_all()
dc.clean_all(de_instance)
dbc_instance.upload_all(de_instance)
dbc_instance.run_sql_alteration_script()