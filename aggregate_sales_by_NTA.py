import pandas as pd
import numpy as np
from sales_by_neighborhood import make_file_names

def main():
    first_file = True
    for file in [file.replace(r'xls','csv') for file in make_file_names()]:
        print 'Aggregating for year = {}, borough = {}'.format(*file.replace('.xls','').split('_'))
        year_boro_dat = pd.read_csv('./../data/sales_data_nta_tagged/{}'.format(file), index_col = 0)
        pvt = year_boro_dat.groupby(['NTA_string', 'year_month']).aggregate({'dollar_per_unit': np.median, 'BBL': len})
        if first_file == True:
            all_data  = pvt
            first_file = False
        else:
            all_data = all_data.append(pvt)
    all_data.sort_index(inplace=True)
    all_data.to_csv('./../data/sales_by_nta_by_month.csv', index=True)

if __name__ == '__main__':
    main()


