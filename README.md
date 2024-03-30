# Multinational Retail Data Centralisation

Table of Contents, if the README file is long


## The project
The aim of this project is to create a system that is capable of retrieving and cleaning data from multiple different sources before uploading it to an SQL database. It should assist users in accessing all of their data from one location, allowing businesses to make more data-driven decisions and get a better understanding of their sales.

This repository contains various methods to access, collate and upload the data. The methods are stored in three different classes; DatabaseConnector, DataExtractor and DataCleaning. The classes themselves are stored in three seperate modules; database_utils, data_extraction and data_cleaning respectively.


While building this repo, I have learned the importance of generalisation when writing class methods. While refactoring, I had noticed entire paragraphs of code that consisted of thematically similar lines and I subsequently created additional class methods to handle this. The 'universal_replace()' method within DataCleaning; for example, is a modified replace() method capable of taking DataFrame positional information as well as remove & replace values and an optional condition as arguments. This allowed me to use a common method to make changes to my database, and this meant that I could loop the common method; passing a dictionary as an argument, thus making the code more scalable and user-friendly.

This has been my one of my first practical experiences of data cleaning, and has demonstrated the importance of being able to efficiently seperate and categorise large quantities of data by using both common and unique characteristics. 

Since the DataExtractor class needed to be capable of extracting data from multiple different formats, it was necessary to create individual methods that will retrieve these types of data, this was achieved through the use of several classmethods[...]

Whilst working on the code for the 'retrieve_stores_data' method, I encountered a problem; there didn't appear to be any documentation supplied with the API information, all I had to work with was a set of headers and an API endpoint. 

Initially, I tried using a 'for' loop in a list comprehension to iterate through each entry, but since the loop had to run 451 times and the connection needed to be inside the loop, this was taking up to 3 minutes to execute.
I tried a few common methods to connect to multiple endpoints at once, but none of them were compatible with the API.
After some research, I discovered that I was able to use the 'ThreadPoolExecutor' method of the 'concurrent.futures' extension to connect to the API multiple times at once to eliminate the need for a loop.

This, however, presented a new problem. Once the database had been extracted using 'ThreadPoolExecutor', several of the row values had changed to NaN and a new series 'message' had been created.
On the NaN rows, the value in 'message' read 'Too Many Requests'.
![alt text](image-3.png)
So now I was capable of retrieving either a complete dataframe in 3 minutes or a dataframe with missing entries in 30 seconds, I figured that I must be able to use both methods together to find a compromise between the two.

I reworked the code with a 'while' loop to trial-and-error the 'chunk_size' until it found the largest chunk that would still collect all of the data.
![alt text](image-1.png)

Once I had incorporated these changes, the runtime for this method was cut down to less than a minute.



A description of the project: what it does, the aim of the project, and what you learned


## Installation instructions
### Suggested pre-requisites:
- conda
- pip
- pgAdmin 4 (optional) 

### Installation
Virtual environment: requirements.txt
To create a virtual environment to run the code

## Usage instructions

First, the dependencies must be installed. The standard  'requirements.txt' file contains the virtual environment.


[...should the user have to create the YAML files?...]
Next, the user will need to create three YAML files containing their credentials for AWS and PostgreSQL, as the DataExtractor class will need to access password-protected information:
- db_creds.yaml
    
    Should contain credentials for the source Amazon RDS database in the following format:
    ```
    {TYPE: postgresql,
    RDS_HOST: 'rds host',
    RDS_PASSWORD: 'rds password',
    RDS_USER: 'rds user',
    RDS_DATABASE: postgres,
    RDS_PORT: 5432}
    ```

- postgres_creds.yaml

    Should contain credentials for the destination PostgreSQL database in the following format:
    ```
    {database_type: 'postgresql'
    database: 'database name', 
    user: 'postgres', 
    password: 'password', 
    host: 'localhost', 
    port: 5432}
    ```

- aws_creds.yaml

    Should contain AWS access keys in the following format:
    ```
    {access_key : 'access key',
    secret_access_key : 'secret access key'}
    ```

Once these YAML files have been created, it is simply a matter of executing the following code, which can be done within a py file stored in the same folder as the YAML files and downloaded project:
```
de_instance = DataExtractor()
dc.clean_all(de_instance)
to_be_uploaded = [
    ('dim_users', de_instance.users_df),
    ('dim_card_details', de_instance.card_df),
    ('dim_store_details', de_instance.stores_df),
    ('dim_products', de_instance.products_df),
    ('orders_table', de_instance.orders_df),
    ('dim_date_times', de_instance.dates_df  )]
for table_name, dataframe in to_be_uploaded:
    dbc.upload_to_db(dataframe, table_name)
```
[...]
## File structure of the project
### database_utils.py
This module contains the DatabaseConnector class, which contains various methods for reading credentials, connecting to Amazon RDS and uploading to PostgreSQL.

### data_extraction.py
This contains the DataExtractor class, 
```
class DataExtractor:
```
which is comprised of several methods capable of retrieving data from multiple different formats, including Amazon S3, Amazon RDS, public PDF documents and APIs. 
```
read_rds_table(table_name, dbc)
```
Takes a table name and an instance of the DatabaseConnector class as arguments and returns the relevant table from Amazon RDS as a Pandas DataFrame.
```
retrieve_pdf_data(link)
```
Takes a PDF link as an argument and returns the information as a Pandas DataFrame.
```
list_number_of_stores(no_of_stores_endpoint, headers_dictionary)
```
Takes an API endpoint and headers dictionary as arguments and returns the number of available records
```
retrieve_stores_data(endpoint, headers)
```
Takes an API endpoint and headers dictionary as arguments, returning a Pandas DataFrame containing the information from the API.
```
extract_from_s3(address)
```
Takes an Amazon S3 address as an argument and casts the information into a Pandas DataFrame


#### data_cleaning.py
The data_cleaning module contains the DataCleaning class, which holds a variety of separate methods for cleaning all of the data retrieved from the various sources.
```
class DataCleaning
```
### License information
