#check later on if all of these imports are necessary

import pandas as pd
import re
import phonenumbers
from database_utils import DatabaseConnector as dbc
from data_extraction import DataExtractor
from phonenumbers import PhoneNumberFormat, NumberParseException

class DataCleaning:
    def format_phonenumber(row):   
    #this try-except will return none for any rows that do not conform
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
                print("The following record(s) did not format", row["first_name"], 
                      row["country_code"], row["last_name"], row["phone_number"])
    

    def clean_user_data(user_data):
        """Takes the 'users' rds dataframe as an argument and correctly formats 
        phone numbers, dates and country codes, and also removes any null or 
        erroneous entries"""
        df = user_data
        print("cleaning user data")
        null_rows = df[df['user_uuid'] == 'NULL'].index
        df.drop(null_rows, inplace=True)
        country_list = df['country'].unique().tolist()
        valid_countries = [x for x in country_list if x.istitle()]        
        invalid_countries_indices = df[~df['country'].isin(valid_countries)].index
        df.drop(invalid_countries_indices, inplace=True)
        df['country'] = df['country'].astype('category')
        df['date_of_birth'] = pd.to_datetime(
            df['date_of_birth'], format='mixed', dayfirst=True
            ).dt.strftime('%Y-%m-%d')    
        df['country_code'] = df['country_code'].replace({'GGB': 'GB'})
        df['address'] = df['address'].str.replace("\n", ", ")    
        to_remove = ["(0)","(",")","+"," ",".","-"]
        for x in to_remove:
            df["phone_number"] = df["phone_number"].str.replace(x, "")       
        #convert extensions to standard format
        df["phone_number"] = df["phone_number"].str.replace("x", ",")
        #handle phone numbers with extensions by splitting them at the "," 
        #and counting the length
        temp_df = df[(df['country_code'] == 'US') & 
                     (df['phone_number'].str.len() > 12)]
        df['phone_number'] = df['phone_number'].str.split(',').str[0]
        temp_df = temp_df[~(temp_df['phone_number'].str.len() < 12)]
        matching_indices = df.index.isin(temp_df.index)
        remove_us_prefix = lambda x: x[2:] if x.startswith('+1') else (
            x[3:] if x.startswith('001') else x)
        
        df.loc[matching_indices, 'phone_number'] = df.loc[
        matching_indices, 'phone_number'].apply(remove_us_prefix)
        df.loc[(
        df["phone_number"].str.startswith("00")) & (
        df["country_code"] == "DE"), "phone_number"] = df.loc[(
           df["phone_number"].str.startswith("00")) & 
           (df["country_code"] == "DE"), 
            "phone_number"].str.replace("00", "49", 1)
        
        df.loc[(df['country_code'] == 'US') & 
               (df['phone_number'].str.startswith('0')) & 
               (df['phone_number'].str.len() < 11), 
               'phone_number'] = df['phone_number'].replace('^0', '1',
                                                             regex=True)
        df["phone_number"] = df.apply(DataCleaning.format_phonenumber, axis=1)
        df = df.drop('index', axis=1, inplace=True)
        return df
    
    def clean_card_data(card_data):
        """Takes the card_data extracted from pdf, removes erroneous/null 
        entries and returns the cleaned dataframe"""
        print("cleaning card data")  
        card_number_df = card_data
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
        stores_df = stores_df.drop('index', axis=1)
        to_remove = stores_df[~stores_df["lat"].isin([None, "N/A"])].index
        stores_df.drop(to_remove, inplace=True)
        stores_df["address"] = stores_df["address"].str.replace("\n", ", ")
        stores_df["staff_numbers"] = stores_df["staff_numbers"].str.replace(r'\D', '', regex=True)
        stores_df["lat"] = stores_df["latitude"]
        stores_df = stores_df.drop("latitude", axis=1)
        stores_df = stores_df.rename(columns={"lat": "latitude"})
        stores_df['continent'] = stores_df['continent'].replace(
            {'eeEurope': 'Europe', 
             'eeAmerica' : 'America'})
        stores_df['opening_date'] = pd.to_datetime(
            stores_df['opening_date'], format='mixed', dayfirst=True
                                            ).dt.strftime('%Y-%m-%d')
        return stores_df
    

    def clean_products_data(products_df):
        """Takes the 'products_df' retrieved from S3 bucket as an 
        argument, fixes spelling mistake in 'removed' series, gets rid
         of any null or erroneous rows, removes unnecessary index and
         formats the 'date_added' series"""
        print("cleaning products data")        
        products_df['removed'] = products_df['removed'].replace(
            {'Still_avaliable': 'Still_available'}
                )
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

        return products_df
    

    @staticmethod
    def convert_product_weights(products_df):
        """Takes the cleaned 'products_df' as an argument and formats the 
        'weight' series. This converts ml, oz and g into kg, and multiplies 
        the weight by 1000 for any products in 'homeware' and 'toys-and-games'
        where the product weight is below 0.004kg(eliminating an input error)
        , returning the adjusted dataframe""" 
        print("converting product weights")
        products_df.loc[
            products_df['weight'].str.endswith(' .', na=False), 'weight'
            ] = products_df.loc[
                products_df['weight'].str.endswith(' .', na=False), 'weight'
                ].str.replace(' .', '')
        
        products_df.loc[
            products_df['weight'].str.endswith('kg', na=False), 'weight'
            ] = (products_df.loc[
                products_df['weight'].str.endswith('kg', na=False), 'weight'
                ].str.replace('kg', '').astype(float))
        #handle multipack weights
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
            products_df.loc[products_df['weight'].str.endswith(unit, na=False), 'weight'] = (
                products_df.loc[products_df['weight'].str.endswith(unit, na=False), 'weight']
                .str.replace(unit, '')
                .astype(float) * factor / 1000
            ) 
        products_df['weight'].astype(float)
        products_df.loc[
            (products_df['category'].isin(['homeware', 'toys-and-games'])) & (
                products_df['weight'] < 0.004), 'weight'] = products_df.loc[(
                products_df['category'].isin(['homeware', 'toys-and-games'])
                        ) & (products_df['weight'] < 0.004), 'weight'] * 1000                  
        return products_df
    
    def clean_orders_table(orders_table):
        """Takes the 'orders_table' retrieved from RDS as an argument, and 
        removes unnecessary columns as required, returning the 
        dataframe"""
        print("cleaning orders table")
        orders_table.drop(['level_0', 'index','first_name', 'last_name', '1'], axis=1, inplace=True)
        return orders_table

    def clean_date_details(date_df):
        """This takes the 'date_details' dataframe as an argument, 
        removes erroneous/null values and casts the 'time_period' as a
        category, returning the df"""
        print("cleaning date details")
        timeperiod_list = date_df['time_period'].unique().tolist()
        valid_timeperiods = [x for x in timeperiod_list if x.istitle()]
        invalid_timeperiod_indices = date_df[~date_df['time_period'].isin(valid_timeperiods)].index
        date_df.drop(invalid_timeperiod_indices, inplace=True)
        date_df['time_period'] = date_df['time_period'].astype('category')
        return date_df