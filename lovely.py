import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt

from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression

# Read in data. Preliminarily drop id, emailid, phoneid, squarefeet, abstract,
# locationtitle, latitude/longitude, and neighborhood/baths null values.
# id is unique for each entry
# emailid has 515 unique entries (no null entries)
# phoneid has 141 unique entries (790 entries total)
# squarefeet has 1002 entries, missing for ~1/3 of data
# abstract: consider later for feature creation or nlp
# neighborhood has 14 null entries
# locationtitle: consider later for feature creations
# createdate: consider later for time series analysis
# latitude/longitude: consider later for analysis

rdf = pd.read_csv("../../../Downloads/oakland_rentals_in_2014.csv")
rdf.createdate = pd.to_datetime(rdf.createdate)
rdf.drop(['id','emailid','phoneid','squarefeet', 'abstract', 'locationtitle', 'createdate', 'latitude', 'longitude'], axis=1, inplace=True)
d = {'t':True, 'f':False, True:True, False:False}
columns = rdf.columns[5:35]
for col in columns:
    rdf[col] = rdf[col].map(d)
rdf = rdf.dropna()

# Identify price outliers
plt.xlabel('Price')
plt.ylabel('Count')
plt.xlim(0,21000)
rdf.price.hist(bins=30)
plt.show()

plt.figure(figsize=(5,10))
plt.ylim(0,7000)
plt.boxplot(rdf.price.values)
plt.show()

rdf.sort('price', ascending=False).head()
# Yup, the four price outliers (price > 9999) look off. Three are repeats of same listing,
# one (id 1119) has abstract "This is a test entry"

# Drop price outliers (four rows)
rdf = rdf[rdf.price < 9999]

rdf_model = pd.get_dummies(rdf, prefix='nbh_', columns=['neighborhood'], sparse=True)

y = rdf_model.pop('price')

rdf_train, rdf_test, y_train, y_test = train_test_split(rdf_model, y)
rfc = RandomForestRegressor(n_estimators=1000)
rfc.fit(rdf_train, y_train)
print "Score of Random Forest: ", rfc.score(rdf_test, y_test)

#Feature importances
print "Feature Importances"
print zip(rdf_model.columns, rfc.feature_importances_)

#lr = LinearRegression(normalize=True, copy_X=True)
#lr.fit(rdf_train, y_train)
#print "Score of Lin Reg: " , lr.score(rdf_test, y_test)
#print "Feature Coefficients"
#print zip(rdf_model.columns, lr.coef_)