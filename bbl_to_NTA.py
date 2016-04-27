import pandas as pd
import numpy as np

def recode_CT2010(code):
    code = str(code).strip()
    if len(code) > 0 and ('.' not in code):
        code = code + '00'
    elif '.' in code:
        code = code.replace('.','')
    code = code.zfill(6)
    return code

borough_abbrevs = ['BK','BX','Mn','QN','SI']
ct_to_nta = pd.read_csv('./../data/census_to_NTA.csv', index_col=None, header=0, usecols = ['2010_borough_code', '2010_census_tract', 'NTA_string'], dtype=str)


BBL_to_NTA = None

for boro in borough_abbrevs:
    print 'Getting map for borough = {}'.format(boro)

    data = pd.read_csv('./../data/nyc_pluto_15v1/{}.csv'.format(boro), index_col=None, header=0, usecols = ['BoroCode','BBL','CT2010'], dtype=str)

    data.dropna(axis=0, subset=['CT2010'],inplace=True)
    data['CT2010'] = data['CT2010'].apply(recode_CT2010)

    print 'Premerge shape for {}: '.format(boro), data.shape

    old_nrows = float(data.shape[0])

    joined_data = data.merge(ct_to_nta,left_on = ['BoroCode','CT2010'], right_on = ['2010_borough_code', '2010_census_tract'])

    print 'Postmerge shape for {}: '.format(boro), joined_data.shape
    print 'Proportion of records retained in merge = %.2f' % (joined_data.shape[0]/old_nrows)

    if BBL_to_NTA is None:
        BBL_to_NTA = joined_data[['BBL', 'NTA_string']]
    else:
        BBL_to_NTA = BBL_to_NTA.append(joined_data[['BBL', 'NTA_string']], ignore_index=True)

BBL_to_NTA.to_csv('./../data/BBL_to_NTA.csv', index=False)
