import pandas as pd
import numpy as np

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

def get_actual_taxi_data():
    ##Finish when Pedro shares taxi data
    return None

def append_simulated_taxi_data(merged):

    num_rows = merged.shape[0]
    merged['taxi_pickup_count'] = np.random.randint(100,10000, size=num_rows)
    merged['taxi_dropoff_count'] = np.random.randint(100,10000, size=num_rows)
    return merged

def merge_taxi_data(simulated_data = True):

    merged = merge_sales_and_permits_to_base_dataframe()

    if simulated_data:
        merged = append_simulated_taxi_data(merged)
    else:
        taxi_data = get_actual_taxi_data()
        merged = pd.merge(merged, taxi_data, on = ['nta_code','year_month'], how='left')
        ##NOTE THIS IS A START TO THE ELSE BLOCK
        ##ADDITIONAL CODE NEEDED DEPENDING ON PEDROS RESULTS

    ##Convert types prior to saving data
    for col in merged.columns:
        if col in ['year_month', 'nta_code', 'nta_string']:
            merged[col] = merged[col].astype(str)
        elif col == 'dollar_per_unit':
            merged[col] = merged[col].astype(float)
        else:
            merged[col] = merged[col].astype(int)

    merged.to_json('./../data/final_nta_month_data.json')
    return merged

if __name__ == '__main__':
    merge_taxi_data(simulated_data = True)
