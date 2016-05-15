import pandas as pd
import numpy as np

def local_regression_estimates(flat_data):
    flat_data.sort(['nta_code','year_month'], inplace=True)
    flat_data.reset_index(drop=True, inplace=True)
    trend_features = [x for x in flat_data.columns if x not in ['year_month','nta_string','nta_code']]
    for index, row in flat_data.iterrows():
        if index % 100 == 0:
            print 'On row ', index
        if row['year_month'] == pd.to_datetime('2010-01', format='%Y-%m'):
            n_rows = 0
        elif row['year_month'] == pd.to_datetime('2010-02', format='%Y-%m'):
            n_rows = 1
        elif row['year_month'] == pd.to_datetime('2010-03', format='%Y-%m'):
            n_rows = 2
        elif row['year_month'] == pd.to_datetime('2010-04', format='%Y-%m'):
            n_rows = 3
        elif row['year_month'] == pd.to_datetime('2010-05', format='%Y-%m'):
            n_rows = 4
        else:
            n_rows = 5
        for col in trend_features:
            slope_feature = col + '_slope'
            if n_rows < 1:
                slope = 0
            else:
                slope = np.polyfit(range(n_rows+1),flat_data.loc[range(index-n_rows,index+1),col],deg=1)[0]
            flat_data.loc[index, slope_feature] = slope
    return flat_data


