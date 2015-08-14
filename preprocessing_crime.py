import pandas as pd
import numpy as np
from crime_dict import crime_dict

from operator import itemgetter # for getting max tuple quickly


def pre_process(cdf):
	cdf.Date = pd.to_datetime(cdf.Date)
	cdf['year'] = pd.DatetimeIndex(cdf.Date).year
	cdf['year_month'] = cdf.Date.map(lambda x: 1000*x.year + x.month)
	cdf.drop(['Idx', 'OIdx', 'CType', 'Desc', 'Beat', 'Addr', 'Src', 'UCR', 'Statute'], axis=1, inplace=True)

	# Remove two categories that don't trend well over time.
	cdf = cdf[np.logical_not(cdf.CrimeCat.isin(['LARCENY_FRAUD', 'LARCENY_FORGERY_COUNTERFEIT']))]

	# Remove 2007 and 2008 because of unparsable duplicates
	cdf = cdf[cdf.year > 2008]

	# Remove NaN's
	cdf.dropna(inplace=True)

	# Map to meta crime categories, imported from crime_dict.py
	''' VIOLENT = 5
    	VEHICLE_THEFT = 4
    	VEHICLE_BREAK_IN = 3
    	NONVIOLENT = 2
    	QUALITY = 1'''
	d = crime_dict(cdf)
	cdf.CrimeCat = cdf.CrimeCat.map(d)

	# Remove meta crime categories that don't trend well over time.
	cdf = cdf[np.logical_not(cdf.CrimeCat.isin(['DOM-VIOL', 'OTHER', 'TRAFFIC_TOWED-VEHICLE', 'WARRANT', 'TRAFFIC_MISDEMEANOR', 'OTHER_MISSING-PERSON', 'OTHER_RUNAWAY']))]

	# Drop easy duplicates
	cdf = cdf.drop_duplicates()

	# Identify remaining OPD_RD's with multiple entries
	duplicates = cdf[cdf.duplicated('OPD_RD')].OPD_RD.unique()

	# For each OPR_RD with mutiple entries, identify the entry with worst crime
	# violent > vehicle_theft > vehicle_break_in > nonviolent > quality
	# And drop other rows

	drop_me = []
	for crime_id in duplicates:
		l = []
		for row in cdf[cdf.OPD_RD == crime_id].itertuples():
			l.append((row[0], row[6]))
		l = [i[0] for i in l if not i == max(l, key=itemgetter(1))]
		drop_me.extend(l)

	cdf.drop(drop_me, inplace=True)
	cdf_dummy = pd.get_dummies(cdf, prefix='CTYPE', columns=['CrimeCat'])

	cdf_dummy.to_csv("data/cdf.csv")