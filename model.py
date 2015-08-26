import psycopg2
import pandas as pd
from sklearn.cluster import KMeans

conn = psycopg2.connect("dbname=oakland user=danaezoule")


'''
Model To-Do:

Feature Engineering Brainstorming:
	Normalization:
	    Normalize by population: none, assume census divisions cover this
	    Normalize by geography (square footage or meterage)
	    Normalize by total crime count

	Geographical:
	    Census tracts, group blocks, or blocks

	Time Group By:
	    Month, quarter, year

	Time features:
	    Count for weekday or weekend
	    Count for time of day (morning, afternoon, eve, night)
	        Split by data. First hypothesis:
	        Morning: 6am-noon
	        Afternoon: noon-6pm
	        Eve: 6pm-midnight
	        Early: midnight-6am
    
	Housing etc:
	    Are Trulia neighborhoods census tracts? Can I get block group info from Trulia?
	    Will the ACS be helpful? Can I get yearly or quarterly ACS information?
	    
	Time component:
	    Create centroids from earliest data, map all points to same centroids
	    Create new centroids for each year (with varied data) as below 
	    
	Data that varies from year to year:
	    If I use it, can I detect similar centroids between years?
	    Should I instead ignore this completely, despite losing Lovely connection?
'''



def clusters(conn):
	'''
	Basic k-means clustering model over all years.

	Input:
	- Psycopg2 connection to database

	Output:
	- Dictionary with one entry per year, containing numbers of clusters for each geometry.

	'''

	cur = conn.cursor()
	cur.execute("SELECT * FROM area_features;")
	df = pd.DataFrame(cur.fetchall())

	cdf = df.copy()
	cdf.columns = ['Group_Block', 'Year', 'Quality', 'Nonviolent', 'Vehicle_Break_In', 'Vehicle_Theft', 'Violent']

	area_index = cdf.sort('Group_Block').Group_Block.unique()

	# Following lines to be used in feature engineering if desired (note change to df columns):
	#cdf.columns = ['Idx', 'OPD_RD', 'Date', 'Time', 'Lat', 'Lng', 'year', 'year_month', 'quality', 'nonviolent', 'car_break_in', 'car_theft', 'violent', 'geom', 'block_group']
	#cdf['day_of_week'] = pd.DatetimeIndex(cdf.Date).dayofweek
	#cdf['day'] = pd.DatetimeIndex(cdf.Date).day
	#cdf['hour'] = [i.hour for i in cdf.Time]

	# Create simple KMeans model, and fit to earliest data set (2009).
	# Keeping centroid constant, predict clusters for subsequent years (2010-2014).
	# Return a dictionary of the predictions (one entry per year).

	columns = ['Quality', 'Nonviolent', 'Vehicle_Break_In', 'Vehicle_Theft', 'Violent']
	km = KMeans(n_clusters=6)
	clus9 = km.fit_predict(cdf[cdf.Year == 2009].sort('Group_Block')[columns])
	clus10 = km.predict(cdf[cdf.Year == 2010].sort('Group_Block')[columns])
	clus11 = km.predict(cdf[cdf.Year == 2011].sort('Group_Block')[columns])
	clus12 = km.predict(cdf[cdf.Year == 2012].sort('Group_Block')[columns])
	clus13 = km.predict(cdf[cdf.Year == 2013].sort('Group_Block')[columns])
	clus14 = km.predict(cdf[cdf.Year == 2014].sort('Group_Block')[columns])

	#clus = {'2009':zip(area_index, clus9), '2010':zip(area_index, clus10), '2011':zip(area_index, clus11), '2012':zip(area_index, clus12), '2013':zip(area_index, clus13), '2014':zip(area_index, clus14) }
	clus = {'2009':clus9, '2010':clus10, '2011':clus11, '2012':clus12, '2013':clus13, '2014':clus14 }
	return clus


if __name__ == "__main__":
    model(conn)