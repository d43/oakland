import pandas as pd
import numpy as np
from crime_dict import crime_dict

from operator import itemgetter # for getting max tuple quickly

def pre_process(cdf):
	'''
	Input:
	- pandas dataframe

	Output:
	- None

	Preprocess data:
		Change Date type to pandas datetime
		Add year column
		Add day_of_week column
		Add hour column
		Add weekend column (one if weekend crime, 0 if not)
		Add morning/workday/evening columns (one if crime occured during, 0 if not)
		Drop unused text columns
		Drop crime categories with inconsistent reporting over time
		Drop 2007/2008 data due to unparsable duplicates
		Remove rows with NaN's
		Map CrimeCat to grouped Crime types (see crime_dict.py for details)
		Deal with duplicate rows (pick worst crime, keep that, delete rest)
		Create dummy variables for CrimeCat, hours, and days of week
		Export data to csv
	'''
	# Change date type to datetime, create year column for index
	cdf.Date = pd.to_datetime(cdf.Date)
	cdf['year'] = pd.DatetimeIndex(cdf.Date).year

	# Create time features
	cdf['day_of_week'] = pd.DatetimeIndex(cdf.Date).dayofweek
	cdf['hour'] = [i.hour for i in pd.to_datetime(cdf.Time)]
	cdf['weekend'] = cdf.day_of_week.isin([5,6])*1

	# Group crimes across segments of the day. Midnight (hr 0) here is excluded as it's used
	# by OPD (Oakland Police Dept) when time is unknown. Determined by observing similar spike
	# in hr_0 across all crime types.
	cdf['morning'] = cdf.hour.isin([1, 2, 3, 4, 5, 6, 7])*1
	cdf['workday'] = cdf.hour.isin([8, 9, 10, 11, 12, 13, 14, 15])*1
	cdf['evening'] = cdf.hour.isin([16, 17, 18, 19, 20, 21, 22, 23])*1

	# Drop text columns
	cdf.drop(['Idx', 'OIdx', 'CType', 'Desc', 'Beat', 'Addr', 'Src', 'UCR', 'Statute'], axis=1, inplace=True)

	# Remove two categories with inconsistent reporting over time
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

	# Remove meta crime categories with inconsistent reporting over time
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

	# Create dummy variables for crime category, hour, and day of week
	cdf_dummy = pd.get_dummies(cdf, prefix='CTYPE', columns=['CrimeCat'])
	cdf_dummy = pd.get_dummies(cdf_dummy, prefix='hr', columns=['hour'])
	cdf_dummy = pd.get_dummies(cdf_dummy, prefix='dow', columns=['day_of_week'])

	# Export to csv
	cdf_dummy.to_csv("data/cdf.csv")