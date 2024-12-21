#check later on if all of these imports are necessary

import pandas as pd
import re
import phonenumbers
from database_utils import DatabaseConnector as dbc
from data_extraction import DataExtractor
from phonenumbers import PhoneNumberFormat, NumberParseException
import numpy as np

class DataCleaning:

    def clean_all(de_instance):
        """Takes an instance of the DataExtractor Class as an argument, runs
         the respective cleaning method for each of the DataFrames and updates
        the de_instance with the cleaned data"""
        de_instance.users_df = DataCleaning.clean_user_data(de_instance.users_df)
        de_instance.card_df = DataCleaning.clean_card_data(de_instance.card_df)
        de_instance.stores_df = DataCleaning.clean_stores_data(de_instance.stores_df)
        de_instance.products_df = DataCleaning.clean_products_data(de_instance.products_df)
        de_instance.orders_df = DataCleaning.clean_orders_table(de_instance.orders_df)
        de_instance.dates_df = DataCleaning.clean_date_details(de_instance.dates_df)
        return de_instance
            
    def clean_user_data(user_data):
        """Takes the 'users' rds dataframe as an argument and correctly formats 
        phone numbers, dates and country codes, and also removes any null or 
        erroneous entries"""
        df = user_data
        print("cleaning user data")
        df = df.drop('index', axis=1)
        df = df[df['user_uuid'] != 'NULL']
        country_list = df['country'].unique().tolist()
        valid_countries = [x for x in country_list if x.istitle()]        
        df = df[df['country'].isin(valid_countries)]
        df['country'] = df['country'].astype('category')
        df['date_of_birth'] = pd.to_datetime(
            df['date_of_birth'], format='mixed', dayfirst=True
            ).dt.strftime('%Y-%m-%d')    

        user_df_replacements = [
            #{'column': 'column name', 'to_replace': 'value(s) to be replaced', 'value': 'replacement value',
            # 'condition': '(optional)filter condition(s)'}
            {'column': 'country_code',  'to_replace': 'GGB', 
             'value': 'GB', 'condition': (df['country_code'] == 'GGB')},
            {'column': 'address', 'to_replace': '\n', 'value': ', ', 'instance': 0},
            {'column': 'phone_number', 'to_replace': ['(0)', '+', ' ', '.', '-', '(', ')'], 
             'value': "", 'instance': 0},
            {'column': 'phone_number', 'to_replace': ['x', 'ext'], 'value': ','},
            {'column': 'phone_number', 'to_replace': '001', 'value': '+1', 
             'condition': (
                    df["phone_number"].str.len() > 11) & (df["country_code"] == "US") & 
                    df['phone_number'].astype(str).str.startswith('001','+1')
                    },
            {'column': 'phone_number', 'to_replace': ["00", "49", "0"], 
             'value': "+49", 'condition': (
                df["country_code"] == "DE") & 
                (df["phone_number"].astype(str).str.startswith(("(0","0","49","00")))},
            {'column': 'phone_number', 'to_replace': ["44", "0"], 'value': "+44", 
             'condition': (
                 df["country_code"] == "GB") &
                (df["phone_number"].astype(str).str.startswith(("0","44")))}
        ]

        df = DataCleaning.universal_batch_replace(df, user_df_replacements)
        DataCleaning.universal_append(df, 'phone_number', '+1', (df['country_code'] == 'US') &  
                         (df['phone_number'].astype(str).str.startswith("011")))
        df["phone_number"] = df.apply(DataCleaning.format_phonenumber, axis=1)
        df["phone_number"] = df["phone_number"].str.replace("-", " ") 

        return df

    def clean_card_data(card_data):
        """Takes the card_data extracted from pdf, removes erroneous/null 
        entries and returns the cleaned dataframe"""
        print("cleaning card data")  
        card_number_df = card_data
        DataCleaning.universal_replace(card_data, 'card_number','?','',instance=0)
        card_number_df = card_number_df[card_number_df['card_number'] != 'NULL']
        card_number_df = card_number_df[card_number_df['expiry_date'].astype(str).str.len() != 10]
        card_number_df['card_provider'] = card_number_df['card_provider'].astype('category')
        card_number_df['date_payment_confirmed'] = pd.to_datetime(
            card_number_df['date_payment_confirmed'], format='mixed', dayfirst=True
            ).dt.strftime('%Y-%m-%d') 
        card_number_df['expiry_date'] = pd.to_datetime(
            card_number_df['expiry_date'], format='%m/%y')
        card_number_df['expiry_date'] = pd.to_datetime(
            card_number_df['expiry_date']).dt.strftime('%m/%y')
        return card_number_df

    def clean_stores_data(stores_df):
        """Takes the 'stores_df' retrieved from the API endpoint as an 
        argument, removes the extra 'index' series,fixes the erroneous 
        'lat/latitude' columns and also formats the 'address', 'continent' 
        and 'opening_date' columns, returning the cleaned dataframe"""
        print("cleaning stores data")
        to_remove = stores_df[~stores_df["lat"].isin([None, "N/A"])].index
        stores_df.drop(to_remove, inplace=True)
        stores_df = stores_df.drop('index', axis=1)
        stores_df_replacements = [
            {'column': 'address', 'to_replace':'\n', 'value': ', ', 'instance': 0},
            {'column': 'continent', 'to_replace':'eeEurope', 'value': 'Europe'},
            {'column': 'continent', 'to_replace':'eeAmerica', 'value': 'America'}
        ]
        DataCleaning.universal_batch_replace(stores_df, stores_df_replacements)
        stores_df["staff_numbers"] = stores_df["staff_numbers"].str.replace(r'\D', '', regex=True)
        #stores_df["longitude"] = stores_df["longitude"].replace('N/A', np.nan)
        #stores_df['longitude'] = pd.to_numeric(stores_df['longitude'])
        stores_df['opening_date'] = pd.to_datetime(
            stores_df['opening_date'], format='mixed', dayfirst=True
                                            ).dt.strftime('%Y-%m-%d')
        return stores_df

    def clean_products_data(products_df):
        """Takes the 'products_df' retrieved from S3 bucket as an 
        argument, fixes spelling mistake in 'removed' series, gets rid
         of any null or erroneous rows, removes unnecessary index and
         formats the 'date_added' series. Finally, calls the 
         convert_product_weights method and returns the dataframe"""
        print("cleaning products data")        
        DataCleaning.universal_replace(products_df, 'removed', 'Still_avaliable',
                                       'Still_available')
        valid_remove_options = ["Still_available", "Removed"]
        invalid_remove_indices = products_df[~products_df['removed']
                                        .isin(valid_remove_options)].index
        products_df.drop(invalid_remove_indices, inplace=True)
        products_df.drop('Unnamed: 0', axis=1, inplace=True)
        products_df['date_added'] = pd.to_datetime(
                                        products_df['date_added'], 
                                        format='mixed', 
                                        dayfirst=True
                                        ).dt.strftime('%Y-%m-%d')
        products_df['category'] = products_df['category'].astype('category')
        products_df = DataCleaning.convert_product_weights(products_df)
        return products_df
    
    @staticmethod
    def convert_product_weights(products_df):
        """Takes the cleaned 'products_df' as an argument and formats the 
        'weight' series. This converts ml, oz and g into kg, handles multipacks
        (eg '6 x 50g') and multiplies the weight by 1000 for any products in 
        'homeware' and 'toys-and-games' where the product weight is below 0.004kg
        (eliminating an input error), returning the adjusted dataframe""" 
        print("converting product weights")
        products_replacements = [
            {'column': 'weight', 'to_replace': ' .', 'value': '', 
             'condition':(products_df['weight'].str.endswith(' .', na=False))},
            {'column': 'weight', 'to_replace': 'kg', 'value': '', 
             'condition': (products_df['weight'].str.endswith('kg', na=False))},
            ]
        DataCleaning.universal_batch_replace(products_df, products_replacements)

        
        split_df = products_df.loc[
            products_df['weight'].str.contains(" x ", na=False), 'weight'
            ].str.split(" x ", expand=True)            
        products_df.loc[
            products_df['weight'].str.contains(" x ", na=False), 'weight'
            ] = (pd.to_numeric(split_df[0]) * pd.to_numeric(
                split_df[1].str.replace('g','')
                )/1000)
        units = {'g': 1, 'oz': 28.35, 'ml': 1}
        for unit, factor in units.items():
            products_df.loc[products_df['weight'].str.endswith(
                unit, na=False), 'weight'] = (
                    products_df.loc[products_df['weight'].str.endswith(
                        unit, na=False), 'weight'].str.replace(
                            unit, '').astype(float) * factor / 1000
            ) 
        products_df['weight'] = products_df['weight'].astype(float)
        products_df.loc[
            (products_df['category'].isin(['homeware', 'toys-and-games'])) & (
                products_df['weight'] < 0.004), 'weight'] = products_df.loc[(
                products_df['category'].isin(['homeware', 'toys-and-games'])
                        ) & (products_df['weight'] < 0.004), 'weight'] * 1000
        products_df['weight'] = products_df['weight'].astype(float)                  
        return products_df
    
    def clean_orders_table(orders_table):
        """Takes the 'orders_table' retrieved from RDS as an argument, and 
        removes unnecessary columns as required, returning the 
        dataframe"""
        print("cleaning orders table")
        orders_table.drop(['level_0', 'index','first_name', 
                           'last_name', '1'], axis=1, inplace=True)
        return orders_table

    def clean_date_details(date_df):
        """This takes the 'date_details' dataframe as an argument, 
        removes erroneous/null values and casts the 'time_period' as a
        category, returning the df"""
        print("cleaning date details")
        timeperiod_list = date_df['time_period'].unique().tolist()
        valid_timeperiods = [x for x in timeperiod_list if x.istitle()]
        invalid_timeperiod_indices = date_df[~date_df['time_period'].isin(
            valid_timeperiods)].index
        date_df.drop(invalid_timeperiod_indices, inplace=True)
        date_df['time_period'] = date_df['time_period'].astype('category')
        return date_df
    
    def format_phonenumber(row):   
        """Called within the 'clean_user_data' method, formats phonenumbers
        internationally by country code, informs the user if there are any entries
        that dont format correctly"""
        try:
            phone = phonenumbers.parse(row["phone_number"], row["country_code"])
            formatted_phone = phonenumbers.format_number(
                phone, PhoneNumberFormat.INTERNATIONAL
                )
            return formatted_phone
        except NumberParseException:
            if row["country_code"] == "DE":
                row["phone_number"] = row["phone_number"].replace("00", "49", 1)
            elif row["country_code"] == "US":
                row["phone_number"] = '1' + row["phone_number"]
            try: 
                phone = phonenumbers.parse(row["phone_number"], row["country_code"])
                formatted_phone = phonenumbers.format_number(phone, PhoneNumberFormat.INTERNATIONAL)
                print("user data: phone numbers formatted")
                return formatted_phone
            except NumberParseException:
                raise Exception("Did not format: ", row["first_name"], 
                      row["last_name"], row["country_code"], row["phone_number"])
                      
    def universal_replace(df, column, remove, insert, condition=None, instance=1):
        """Replaces data within entries of a pandas dataframe with an optional condition:
            df : Relevant DataFrame
            column : Series containing the data that we want to replace
            remove : Data to be replaced; can either be a string or list of strings
            insert : Data to replace removed data
            condition :  (Optional)What is specific about the data that we want to change ?
                            e.g. (df['phone_number'].str.startswith('?'))
        """
        if condition is None:
            condition = pd.Series([True]*len(df), index=df.index)

        if instance == 0:
            for character in remove:
                df[column] = df[column].astype(str).str.replace(character, insert)
            return df

        if isinstance(remove,str):
            df.loc[condition, column] = df.loc[condition, column].str.replace(remove,insert,instance)
            return df
        elif isinstance(remove, list):
            for value in remove:
                mask = (df[column].astype(str).str.startswith(value)) & condition
                df.loc[mask, column] = df.loc[mask, column].str.replace(value, insert, instance)
            return df
   
    def universal_batch_replace(df, replacements):
        """Takes a dataframe and list of dictionaries as arguments, and makes 
        multiple replacements within the dataframe using the values from the 
        dictionary list
        Dictionary syntax is as follows: 
        {'column': 'column name',
        'to_replace': 'value(s) to be removed'(str or list),
        'value': 'replacement value',
        'condition': (optional)'filter condition',
        'instance': (optional)'instance; default 1 for 1st instance or 0 for all'        
        }, {'column': ...},..."""
        for replacement in replacements:
            if 'condition' in replacement:
                DataCleaning.universal_replace(
                    df, replacement['column'], 
                    replacement['to_replace'], 
                    replacement['value'], 
                    replacement['condition'])
            elif 'instance' in replacement:
                DataCleaning.universal_replace(
                    df, replacement['column'], 
                    replacement['to_replace'], 
                    replacement['value'], 
                    instance=replacement['instance'])
            else:
                DataCleaning.universal_replace(
                    df, 
                    replacement['column'], 
                    replacement['to_replace'], 
                    replacement['value'])
        return df

    def universal_append(df,column,insert,condition=None):
        """Adds a string at the beginning of entries, either 
        at the positions specified by the optional 'condition' argument
        or the entire series as a default"""
        if condition is None:
            condition = pd.Series([True]*len(df), index=df.index)
        df.loc[condition, column] = insert + df.loc[condition, column]
        return df

    

