import pandas as pd
from datetime import datetime
import itertools
import numpy as np
import urllib2
import json
from nyc_geoclient import Geoclient
import re

def get_clean_street_name(address):
    address = str(address)
    address_list = address.split(None,1)
    if len(address_list)>1:
        street_name = address_list[1]
        street_name = street_name.strip()
        street_name = street_name.replace(r'\s+',' ')
        street_name = street_name.replace('stret','street')
        return street_name
    else:
        return np.NaN

def get_clean_street_number(address):
    address = str(address)
    if len(address)>0:
        street_number = re.split(r'\s+', address)[0]
        if street_number.isdigit():
            return street_number
        elif re.search(r'[^0-9\-]',street_number):
            ## Some character other than digit or hyphen
            ## Note hyphens are valid in street numbers
            ## Arbitrarily split on this character and take
            ## first component
            street_number_lists = re.split(r'[^0-9\-]', street_number)
        else:
            ## Not a digit, and no non-digit-or-hyphen characters
            return street_number
            return street_number_lists[0]
    else:
        return np.NaN

def get_year_month(datetime_obj):
    return str(datetime_obj.year) + '-' + str(datetime_obj.month)


def make_file_names():
    years = range(2011,2014)
    boroughs = ['bronx', 'manhattan','queens','statenisland','brooklyn']
    year_boroughs = list(itertools.product(*[years, boroughs]))
    file_names = ['{}_{}.xls'.format(year, boro) for year, boro in year_boroughs]
    return file_names

def clean_sales_dataframe(boro_year_data):
    ###Clean column names and select target features
    boro_year_data.columns = boro_year_data.columns.map(lambda x: x.strip())
    boro_year_data = boro_year_data[['BOROUGH','BLOCK','LOT', 'ADDRESS', 'RESIDENTIAL UNITS', 'COMMERCIAL UNITS','SALE PRICE','SALE DATE']]

    ###Filter data- drop sales of buildings with commercial units or with
    ###no residential units
    boro_year_data = boro_year_data.loc[boro_year_data['COMMERCIAL UNITS'] == 0,:]
    boro_year_data = boro_year_data.loc[boro_year_data['RESIDENTIAL UNITS'] >= 1,:]

    ###Drop feature (since now all records have 0 commercial units)
    boro_year_data.drop('COMMERCIAL UNITS', axis=1, inplace=True)

    ### Note a substantial number of properties sell for $0, $1, or $100
    ### Drop these records
    boro_year_data = boro_year_data.loc[boro_year_data['SALE PRICE'] >= 1000,:]

    return boro_year_data

def make_new_sales_features(boro_year_data):
    ###Construct new features:
    ###     - dollar_per_unit- to normalize price by number of units
    ###     - year_month- to support aggregation by month
    boro_year_data['dollar_per_unit'] = boro_year_data['SALE PRICE'] / boro_year_data['RESIDENTIAL UNITS']
    boro_year_data['year_month'] = boro_year_data['SALE DATE'].apply(get_year_month)

    ###Create street number and street name for API call
    boro_year_data['street_number'] = boro_year_data['ADDRESS'].apply(get_clean_street_number)
    boro_year_data['street_name'] = boro_year_data['ADDRESS'].apply(get_clean_street_name)

    ###Create BBL for merge with bbl_to_nta
    boro_year_data['BBL'] = boro_year_data["BOROUGH"].map(str) + boro_year_data["BLOCK"].map(lambda x: str(x).zfill(5)) + boro_year_data["LOT"].map(lambda x: str(x).zfill(4))

    return boro_year_data

def get_nta_name_through_api(row, geoclient_wrapper):
    ###Borough code map for API call
    borough_number_map = {1:'Manhattan', 2:'Bronx', 3:'Brooklyn', 4:'Queens', 5:'Staten Island'}

    ###Get address components
    boro = borough_number_map[row['BOROUGH']]
    street_num = row['street_number']
    street_name = row['street_name']

    ###Query API
    try:
        query_results = g.address(street_num, street_name, boro)
        if 'ntaName' in query_results:
             return query_results['ntaName'], None
    except Exception as e:
        return np.NaN, e.message


def main():

    ###Instantiate geoclient wrapper
    g = Geoclient('7cb56bda', '51f262e341572a09e73aa32eb1dda793')

    ###Read in data
    bbl_to_nta = pd.read_csv('./../data/BBL_to_NTA.csv',dtype=str)
    file_names = make_file_names()

    bad_api_calls = {}

    for file in file_names:
        bad_api_calls[file] = {}

        if file[0:4] == '2010':
            skiprows_n = 3
        else:
            skiprows_n = 4
        boro_year_data = pd.read_excel('./../data/sales_data/{}'.format(file),sheetname=0, skiprows=range(skiprows_n))

        ###Clean and make new features
        boro_year_data = clean_sales_dataframe(boro_year_data)
        boro_year_data = make_new_sales_features(boro_year_data)

        ###First merge on BBL
        print 'Merging for year = {}, borough = {}'.format(*file.replace('.xls','').split('_'))
        merged = pd.merge(boro_year_data, bbl_to_nta, on='BBL', how='left')

        ###Then use API to fill missed NTA_strings

        print 'Number missing NTA_strings after merge: ', sum(merged['NTA_string'].isnull())
        for index, row in merged.iterrows():
            if pd.isnull(row['NTA_string']):
                query_results = get_nta_name_through_api(row, g)
                merged.loc[index,'NTA_string'] = query_results[0]
                if query_results[1] not None:
                    bad_api_calls[file][index] = query_results[1]

        print 'Number missing NTA_strings after API call: ', sum(merged['NTA_string'].isnull())
        print 'Number of bad API calls: ', len(bad_api_calls[file]), '\n'

        ###Finally save NTA tagged data
        merged.to_csv('./../data/sales_data_nta_tagged/{}.csv'.format(file.replace('.xls','')), index=True)

    json.dump(bad_api_calls, open("./../data/sales_data_nta_tagged/bad_api_calls.txt",'w'))

if __name__ == '__main__':
    main()
