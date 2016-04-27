import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

def clean_bbl(Borough_Block_Lot):
    if not (Borough_Block_Lot.isdigit() and len(Borough_Block_Lot) == 10):
        return np.nan
    elif Borough_Block_Lot == '0000000000':
        return np.nan
    else:
        return Borough_Block_Lot


def get_year_month(issuance_date):
    formats = ['%Y-%m-%d', '%m/%d/%Y %H:%M:%S']

    format_found = False
    for format in formats:
        try:
            as_date_time = datetime.strptime(issuance_date, format)
            year = as_date_time.year
            month = as_date_time.month
            format_found = True
        except:
            continue

    if format_found:
        if int(year) <= 2009:
            ### This is a missing value, and will be dropped later
            return np.nan
        else:
            return '{}-{}'.format(year, month)
    else:
        ### This is a missing value, and will be dropped later
        return np.nan

def visualize_bbl_dist_by_merge(permits_merge):
    '''
    Function to visualize successful vs. unsuccessful merge results by BBL.
    Used to assess whether there is bias in merge results based on type
    of tax lot:
    From PLUTO data dictionary:

    TAX LOT NUMBER - TYPE OF LOT
    ----------------------------
    1-999 - Traditional Tax Lots
    1001-6999 - Condominium Unit Lots
    7501-7599 - Condominium Billing Lots
    8000-8899 - Subterranean Tax lots
    8900-8999 - DTM Dummy Tax Lots
    9000-9899 - Air Rights Tax Lots
    '''

    not_na = permits_merge.loc[permits_merge['NTA_string'].notnull(),:]
    is_na = permits_merge.loc[permits_merge['NTA_string'].isnull(),:]

    plt.figure()
    plt.hist(not_na['Borough_Block_Lot'].apply(lambda x: int(x)).values, bins=100, color='g', alpha = 0.3, label='Merged')
    plt.hist(is_na['Borough_Block_Lot'].apply(lambda x: int(x)).values, bins=100, color='b', alpha = 0.3, label='Not merged')
    plt.legend(loc='best')
    plt.show()

def clean_permit_app_data(permits):
    permits.dropna(axis=0, how='any',inplace=True)

    print 'Formatting Permit_Issuance_Date...'

    permits['Permit_Issuance_Date'] = permits['Permit_Issuance_Date'].apply(get_year_month)

    print 'Formatting BBL...'

    permits['Borough_Block_Lot'] = permits['Borough_Block_Lot'].apply(clean_bbl)

    permits.dropna(axis=0, how='any',inplace=True)

    return permits

def main(visualize=False):

    ###Read in and clean permits data
    print 'Reading in permit applications...'

    permits = pd.read_csv('./../data/Multi_Agency_Permits.csv',header=0,usecols = ['Borough_Block_Lot', 'Permit_Issuance_Date', 'Permit_Type_Description'], dtype=str)

    permits = clean_permit_app_data(permits)

    print 'Pre-merge permits shape: ', permits.shape

    ###Merge in NTA_string

    BBL_to_NTA = pd.read_csv('./../data/BBL_to_NTA.csv', dtype=str)

    permits_merge = permits.merge(BBL_to_NTA, left_on=['Borough_Block_Lot'], right_on=['BBL'], how='left')

    permits_merge = permits_merge[['Permit_Issuance_Date', 'Permit_Type_Description', 'NTA_string','Borough_Block_Lot']]

    ### To visualize distribution of BBLs based on merged success
    if visualize == True:
        visualize_bbl_dist_by_merge(permits_merge)

    permits_merge.drop('Borough_Block_Lot', axis = 1, inplace = True)
    old_nrows = float(permits_merge.shape[0])

    permits_merge.dropna(axis=0, how='any', inplace=True)

    print 'Proportion of records retained in merge = %.2f' % (permits_merge.shape[0]/old_nrows)

    counts = permits_merge.pivot_table(index = ['Permit_Issuance_Date', 'NTA_string'], columns='Permit_Type_Description', aggfunc=len, fill_value=0)

    print 'Final counts shape (aggregating by date/nta)', counts.shape

    counts.to_csv('./../data/permits_by_NTA_by_month.csv', index=True)

if __name__ == '__main__':
    main()
