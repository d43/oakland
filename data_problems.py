import pandas as pd
import numpy as np

'''
Investigating changes in OPD reporting starting in April of 2014
'''

#Load in data and pre-process
cdf = pd.read_csv("data/OPD_150308.csv")
cdf.Date = pd.to_datetime(cdf.Date)
cdf['year'] = pd.DatetimeIndex(cdf.Date).year

#identify duplicated rows
duplicates = cdf[cdf.duplicated('OPD_RD')].OPD_RD.unique()


output = []
for row in cdf[cdf.OPD_RD.isin(duplicates)].itertuples():
    output.append((row[2], row[15]))
output = sorted(output, key=lambda tup: tup[0], reverse=True)