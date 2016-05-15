import pandas as pd
import numpy as np
import json

def get_allowed_year_months():
    ##Construct allowed year-month keys
    allowed = []
    for year in range(2010,2014):
        for month in range(1,13):
            allowed.append('{}-{}'.format(year,month))
    return allowed

def get_NTA_codes():
    codes = pd.read_csv('./../data/census_to_NTA.csv', usecols=['NTA_code','NTA_string'])
    codes.drop_duplicates(inplace=True)
    codes.columns = [x.lower() for x in codes.columns]
    return codes

def make_base_dataframe():
    ##Read in NTA codes from census to NTA map

    codes = get_NTA_codes()

        ##Construct all allowed NTA_code year-month pairs

    allowed = get_allowed_year_months()
    base_df = []
    for year_month in allowed:
        for row in codes.itertuples():
            base_df.append([year_month,row[1],row[2]])

    base_df = pd.DataFrame(base_df)
    base_df.columns = ['year_month','nta_code','nta_string']

    return base_df

def read_and_subset_permit_data():

    ##Read in data and format column names
    permits = pd.read_csv('./../data/permits_by_NTA_by_month.csv')
    permits.rename(columns={'Permit_Issuance_Date':'year_month'}, inplace=True)
    permits.columns = [x.strip('.').replace(' ','_').lower() for x in permits.columns]

    ##Select only allowed year_months (2010-2013)
    allowed = get_allowed_year_months()
    year_bool = permits['year_month'].apply(lambda x: x in allowed)
    permits = permits.loc[year_bool,:]

    ##Group by NTA_string and find mean counts for all permit types
    grouped = permits.groupby('nta_string').aggregate(np.mean)

    ##Get standard deviation for mean counts by neighborhood
    stdevs = np.std(grouped, axis=0)

    ##Select the permit types with the greatest std (most interesting)
    ##for inter-neighborhood comparison.

    stdevs = stdevs[np.argsort(stdevs)]
    targets = [x for x in stdevs.index[-14:]]
    targets.extend(['nta_string', 'year_month'])
    permits = permits.loc[:,targets]

    ##Merge in NTA codes
    codes = get_NTA_codes()
    merged = pd.merge(codes, permits, on='nta_string', how='inner')

    return merged

def clean_and_merge_sales_data_with_NTA_codes():

    ##Get codes

    codes = get_NTA_codes()

    ##Read sales data and formate columns
    sales = pd.read_csv('./../data/sales_by_nta_by_month.csv')
    sales.columns = [x.lower() for x in ['NTA_string','year_month','dollar_per_unit','total_num_sales']]

    ##Merge in codes
    merged = pd.merge(codes, sales, on='nta_string', how='inner')

    return merged

def merge_sales_and_permits_to_base_dataframe():

    ##Make base dataframe

    base_df = make_base_dataframe()

    print 'Base dataframe shape: ', base_df.shape

    ##Read in and clean/format sales and permit data

    sales = clean_and_merge_sales_data_with_NTA_codes()

    permits = read_and_subset_permit_data()

    ##Merge sales and permit data

    merged = pd.merge(base_df, sales, on = ['nta_code','year_month'], how='left')

    print 'Base dataframe shape after merging sales: ', merged.shape

    merged = pd.merge(merged, permits, on = ['nta_code','year_month'], how='left')

    print 'Base dataframe shape after merging permits: ', merged.shape

    ##Fill NaN values introduced in merge due to 0 sales/permits for NTA/year_month

    print 'Number of records with null values (introduced due to 0 permits/sales):', np.sum(merged.isnull().any(axis=1))

    merged.fillna(0, inplace=True)

    print 'Number of records with null values after fill:', np.sum(merged.isnull().any(axis=1))

    ##Drop duplicate columns

    merged.drop(['nta_string_y', 'nta_string'], axis=1, inplace=True)

    merged.rename(columns={'nta_string_x':'nta_string'},inplace=True)

    return merged


def strip_zero_from_month(year_month):
    '''
    Problem: The other datasets have both YYYY-M and YYYY-MM year_months.
    Solution: Drop leading zeros from the taxi data for merge, then add back to ensure consistent format
    '''
    year,month = year_month.split('-')
    if month[0] == "0":
        month = month[1]
    return '-'.join([year, month])

def add_zero_to_month(year_month):
    '''
    Add zero back in to final year_month
    '''
    year,month = year_month.split('-')
    if len(month) == 1:
        month = '0' + month
    return '-'.join([year, month])


def read_output_from_MR(filepath):
    df = pd.read_csv(filepath, sep=r'[,\t]', header=None, names=['year_month', 'nta_code', 'count','trip_type'],engine='python')

    df['year_month'] = df['year_month'].apply(strip_zero_from_month)

    pivoted = pd.pivot_table(df, index=['nta_code','year_month'], columns='trip_type', values='count')
    pivoted.columns = pivoted.columns.get_level_values(0)
    pivoted.reset_index(inplace=True)
    return pivoted


def get_actual_taxi_data(filepathlist):
    first_year = True
    for filepath in filepathlist:
        if first_year:
            taxi_data = read_output_from_MR(filepath)
            first_year = False
        else:
            taxi_data = taxi_data.append(read_output_from_MR(filepath))
    return taxi_data

def append_simulated_taxi_data(merged):

    num_rows = merged.shape[0]
    merged['pickup'] = np.random.randint(100,10000, size=num_rows)
    merged['dropoff'] = np.random.randint(100,10000, size=num_rows)
    return merged

def merge_taxi_data(filepathlist, simulated_data = True):

    merged = merge_sales_and_permits_to_base_dataframe()

    if simulated_data:
        merged = append_simulated_taxi_data(merged)
    else:
        taxi_data = get_actual_taxi_data(filepathlist)
        print taxi_data.head()
        merged = pd.merge(merged, taxi_data, on = ['nta_code','year_month'], how='left')
        print "Number NaN after merge", merged.isnull().any(axis=1).sum()
        merged.fillna(0,inplace=True)

    ###Get NYC averages

    nyc_means = merged.groupby('year_month').mean()
    nyc_means['nta_code'] = 'NYC_avg'
    nyc_means['nta_string'] = 'NYC average'
    nyc_means.reset_index(inplace=True)
    merged = merged.append(nyc_means)

    ##Convert types prior to returning data
    for col in merged.columns:
        if col in ['year_month', 'nta_code', 'nta_string']:
            merged[col] = merged[col].astype(str)
        else:
            merged[col] = merged[col].astype(float)

    ##Pad the month in the year_month:
    merged['year_month'] = merged['year_month'].apply(add_zero_to_month)
    merged.to_csv('./../data/view_data_flat.csv', index=False)
    return merged

def nest_dict_from_df(frame):
    return_dict = {}
    for index, row in frame.iterrows():
        if row['nta_code'] not in return_dict:
            return_dict[row['nta_code']] = {}
        values_for_inner_dict = row.drop(['nta_code','year_month']).to_dict()
        if row['nta_code'] != 'NYC_avg':
            for key in [x for x in values_for_inner_dict.keys() if x not in ['nta_code','year_month','dollar_per_unit','nta_string']]:
                values_for_inner_dict[key] = int(values_for_inner_dict[key])
        return_dict[row['nta_code']][row['year_month']] = values_for_inner_dict
    return return_dict

def save_merged_data(filepathlist, simulated_data = True):
        ##Reset index to year_month and nta_code

    merged =  merge_taxi_data(filepathlist, simulated_data)

    merged_dict = nest_dict_from_df(merged)

    with open('./../data/final_nta_month_data.json', 'w') as output:
        json.dump(merged_dict, output)
