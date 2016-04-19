import pandas as pd
import numpy as np
from datetime import datetime

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
            pass

    if format_found:
        return '{}-{}'.format(year, month)
    else:
        print issuance_date


permits = pd.read_csv('./../data/Multi_Agency_Permits.csv',header=0,usecols = ['Borough_Block_Lot', 'Permit_Issuance_Date', 'Permit_Type_Description'])

BBL_to_NTA = pd.read_csv('./../data/BBL_to_NTA.csv')

permits_merge = permits.merge(BBL_to_NTA, left_on=['Borough_Block_Lot'], right_on=['BBL'])
permits_merge = permits_merge[['Permit_Issuance_Date', 'Permit_Type_Description', 'NTA_string']]

permits_merge.dropna(subset=['Permit_Issuance_Date'], inplace=True)

permits_merge['Permit_Issuance_Date'] = permits_merge['Permit_Issuance_Date'].apply(lambda x: get_year_month(x))

counts = permits_merge.pivot_table(index = ['Permit_Issuance_Date', 'NTA_string'], columns='Permit_Type_Description', aggfunc=len, fill_value=0)

counts.to_csv('./../permits_by_NTA_by_month.csv', index=True)
