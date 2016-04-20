import pandas as pd
from datetime import datetime
import itertools
import numpy as np

def get_year_month(datetime_obj):
    return str(datetime_obj.year) + '-' + str(datetime_obj.month)

bbl_to_nta = pd.read_csv('./../data/BBL_to_NTA.csv',dtype=str)

years = range(2010,2014)
boroughs = ['bronx', 'manhattan','queens','statenisland','brooklyn']
year_boroughs = list(itertools.product(*[years, boroughs]))

file_names = ['{}_{}.xls'.format(year, boro) for year, boro in year_boroughs]

all_data = None

for file in file_names:
    if file[0:4] == '2010':
        skiprows_n = 3
    else:
        skiprows_n = 4

    boro_year_data = pd.read_excel('./../data/sales_data/{}'.format(file),sheetname=0, skiprows=range(skiprows_n))
    boro_year_data.columns = boro_year_data.columns.map(lambda x: x.strip())
    boro_year_data = boro_year_data[['BOROUGH','BLOCK','LOT', 'RESIDENTIAL UNITS', 'COMMERCIAL UNITS','SALE PRICE','SALE DATE']]
    boro_year_data = boro_year_data.loc[boro_year_data['COMMERCIAL UNITS'] == 0,:]
    boro_year_data = boro_year_data.loc[boro_year_data['RESIDENTIAL UNITS'] >= 1,:]
    boro_year_data.drop('COMMERCIAL UNITS', axis=1, inplace=True)

    ### Note a substantial number of properties sell for $0, $1, or $100
    ### Drop these records
    boro_year_data = boro_year_data.loc[boro_year_data['SALE PRICE'] >= 1000,:]
    boro_year_data['dollar_per_unit'] = boro_year_data['SALE PRICE'] / boro_year_data['RESIDENTIAL UNITS']
    boro_year_data['year_month'] = boro_year_data['SALE DATE'].apply(get_year_month)
    boro_year_data.drop('SALE DATE', axis=1, inplace=True)
    boro_year_data['BBL'] = boro_year_data["BOROUGH"].map(str) + boro_year_data["BLOCK"].map(lambda x: str(x).zfill(5)) + boro_year_data["LOT"].map(lambda x: str(x).zfill(4))

    print 'Pre-merge shape for {}'.format(file), boro_year_data.shape
    merged = pd.merge(boro_year_data, bbl_to_nta, on='BBL', how='inner')
    print 'Post-merge shape for {}'.format(file), merged.shape

    grouped = merged.groupby(['NTA_string', 'year_month']).aggregate({'dollar_per_unit': np.median, 'BBL': len})


    if all_data is None:
        all_data = grouped
    else:
        all_data = all_data.append(grouped)

all_data.to_csv('./../data/NTA_month_median_price_per_unit.csv', index=True)
