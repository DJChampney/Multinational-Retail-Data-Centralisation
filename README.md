# Multinational Retail Data Centralisation

### Table of Contents
- The project
- Installation instructions
- [Usage instructions](https://github.com/DJChampney/Multinational-Retail-Data-Centralisation/wiki/Usage-Instructions)
- [File structure of the project](https://github.com/DJChampney/Multinational-Retail-Data-Centralisation/wiki/File-Structure)
- License information


## The project
The aim of this project is to create a system that is capable of retrieving and cleaning data from multiple different sources before uploading it to an SQL database. It should assist users in accessing all of their data from one location, allowing businesses to make more data-driven decisions and get a better understanding of their sales.

More specifically, the exact tasks that the methods are designed to perform are:

- Access stored user credentials
- Retrieve a table of users and addresses from a given Amazon RDS instance
- Retrieve a table of card details and user IDs from a PDF document using tabula
- Retrieve a table of store details from an API with given endpoints and headers
- Retrieve a table of product details from an Amazon S3 bucket using a provided S3 address
- Output a table of date details from a `json` file using a given HTML
- Clean all retrieved data
- Upload all cleaned data to an SQL database called `sales_data`
- Run a default SQL query to format data types, implement a list of specified custom features to the database and add primary & foreign keys to the correct columns of each table in order to create a star-based schema
- Run a selection of sample SQL queries on the new database

These tasks are carried out by a set of python class methods, contained in the attached modules. More details are available in the __File Structure__ section below. 

While building this repo, I have learned the importance of generalisation when writing class methods. While refactoring, I had noticed entire paragraphs of code that consisted of thematically similar lines and I subsequently created additional class methods to handle this. The ```universal_replace()``` method within ```DataCleaning```; for example, is a modified ```replace()``` method capable of taking DataFrame positional information as well as remove & replace values and an optional condition as arguments. This enabled me to use a common method to make mass changes to my DataFrame, which meant that I could loop the common method; passing a dictionary as an argument, thus making the code more scalable and user-friendly.


## Installation instructions
### Suggested pre-requisites:
- conda
- pip
- pgAdmin 4 (optional) 
- requirements.txt extension(mentioned in __Installation__ section)

### Installation

Before the classes and their contained methods are used, the dependencies must be installed. The standard  'requirements.txt' file contains the virtual environment.

On the command line, navigate to the downloaded repository folder and run:
```
$ pip install -r requirements.txt
```
It is also necessary to create a blank SQL database, pgadmin4 will work for this.



### License information
Copyright (c) 2024 David Champney

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE
