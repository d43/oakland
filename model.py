import psycopg2
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

'''
Model To-Do:

Feature Engineering Brainstorming:
	Normalization:
	    Normalize by population: none, assume census divisions cover this
	    Normalize by geography (square footage or meterage)
	    Normalize by total crime count
	    Normalize across all features (dimensionality)

	Geographical:
	    Census tracts, group blocks, or blocks
	    	Group blocks?

	Time Group By:
	    Month, quarter, year
	    	Year!

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
	    Should I instead ignore this completely, despite losing rental data?
	    
	Final model:
	    Run several times, pick best clusters- min of objective function (within cluster
	    variation, provided by pairwise squared Euclidean distances between observations)
	    Dendrogram/hierarchical clustering to pick k
'''



def clusters(conn):
	'''
	Basic k-means clustering model over all years.

	Input:
	- Psycopg2 connection to database

	Output:
	- Dictionary with one entry per year, containing numbers of clusters for each geometry.

	'''

	print "Modeling: connecting to DB"

	cur = conn.cursor()
	cur.execute("SELECT * FROM area_features;")
	df = pd.DataFrame(cur.fetchall())
	cdf = df.copy()

	cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='area_features';")
	col_df = pd.DataFrame(cur.fetchall())
	col_df.columns = ['labels']
	cdf.columns = col_df['labels'].tolist()

	print "Modeling: scaling data"
	# Scale data
	df_index = pd.DataFrame(cdf.pop('ogc_fid'))
	df_index['year'] = cdf.pop('year')

	ss = StandardScaler()
	scaled_df = pd.DataFrame(ss.fit_transform(cdf))
	scaled_df.columns = cdf.columns

	print "Modeling: PCA"
	# PCA
	pca = PCA(n_components = 5)
	pca_df = pd.DataFrame(pca.fit_transform(scaled_df))

	pca_df['ogc_fid'] = df_index['ogc_fid']
	pca_df['year'] = df_index['year']

	# Create simple KMeans model, and fit to earliest data set (2009).
	# Keeping centroid constant, predict clusters for subsequent years (2010-2014).
	# Return a dictionary of the predictions (one entry per year).

	#columns = ['Quality', 'Nonviolent', 'Vehicle_Break_In', 'Vehicle_Theft', 'Violent']

	print "Modeling: kMeans"
	km = KMeans(n_clusters=7)
	clus9 = km.fit_predict(pca_df[pca_df.year == 2009].sort('ogc_fid'))
	clus10 = km.predict(pca_df[pca_df.year == 2010].sort('ogc_fid'))
	clus11 = km.predict(pca_df[pca_df.year == 2011].sort('ogc_fid'))
	clus12 = km.predict(pca_df[pca_df.year == 2012].sort('ogc_fid'))
	clus13 = km.predict(pca_df[pca_df.year == 2013].sort('ogc_fid'))
	clus14 = km.predict(pca_df[pca_df.year == 2014].sort('ogc_fid'))

	# Aggregate yearly clusters into one dictionary.
	clus = {'2009':clus9, '2010':clus10, '2011':clus11, '2012':clus12, '2013':clus13, '2014':clus14 }
	return clus
