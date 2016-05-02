import pandas as pd
import numpy as np
from sales_by_neighborhood import make_file_names

def main():
    first_file = True
    for file in [file.replace(r'xls','csv') for file in make_file_names()]:
        print 'Aggregating for year = {}, borough = {}'.format(*file.replace('.xls','').split('_'))
        year_boro_dat = pd.read_csv('./../data/sales_data_nta_tagged/{}'.format(file), index_col = 0)

        num_rows = year_boro_dat.shape[0]
        year_boro_dat.dropna(axis=0,how='any',inplace=True)
        num_rows_no_null = float(year_boro_dat.shape[0])
        print 'NTA_string coverage = %.2f' % (num_rows_no_null/num_rows)

        ##Note multiple versions of NTA name for Hudson Yards
        ##Correct this name mismatch.

        year_boro_dat.replace('Hudson Yards-Chelsea-Flatiron-Union Square','Hudson Yards-Chelsea-Flat Iron-Union Square',inplace=True)

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


