from merge_sample_data import *
from compute_slopes import *


if __name__ == '__main__':

    print 'Merging taxi, permits and sales data...'
    years = range(2010,2014)
    filepathlist = ['./../data/taxi_from_map_reduce/{}-taxi-data.txt'.format(year) for year in years]
    save_merged_data(filepathlist, simulated_data=False)

    print 'Computing slopes and saving flat file...'
    flat_data = pd.read_csv('./../data/view_data_flat.csv', index_col=False)
    flat_data['year_month'] = pd.to_datetime(flat_data['year_month'], format='%Y-%m')
    slopes_appended = local_regression_estimates(flat_data)
    slopes_appended.to_csv('./../data/with_slopes.csv', index=False)
