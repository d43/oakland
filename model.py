import psycopg2
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

def clusters(conn):
	'''
	Basic k-means clustering model over all years.

	Input:
	- Psycopg2 connection to database

	Output:
	- Dictionary with one entry per year, containing numbers of clusters for each geometry.

	Connects to database, gets data, scales data (via sklearn StandardScalar), runs PCA
	(sklearn PCA), and models (sklearn KMeans) for each year.

	'''

	print "Modeling: connecting to DB"
	# Get Data
	cur = conn.cursor()
	cur.execute("SELECT * FROM area_features;")
	df = pd.DataFrame(cur.fetchall())
	cdf = df.copy()
	# Get Data Labels
	cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='area_features';")
	col_df = pd.DataFrame(cur.fetchall())
	col_df.columns = ['labels']
	cdf.columns = col_df['labels'].tolist()

	# Pop index labels off prior to scaling and PCA
	df_index = pd.DataFrame(cdf.pop('ogc_fid'))
	df_index['year'] = cdf.pop('year')

	print "Modeling: scaling data"
	# Scale all features

	ss = StandardScaler()
	scaled_df = pd.DataFrame(ss.fit_transform(cdf))
	scaled_df.columns = cdf.columns

	# PCA
	# Fit PCA on 2009 data, then transform entire set
	print "Modeling: PCA"
	n_components = 19
	pca = PCA(n_components = n_components)
	pca.fit(scaled_df[df_index.year == 2009])
	pca_df = pd.DataFrame(pca.transform(scaled_df))

	# Reattach year and ogc_fid columns
	pca_df['year'] = df_index['year']
	pca_df['ogc_fid'] = df_index['ogc_fid']

	# Pull out non-index columns to pass into model
	columns =  pca_df.columns.tolist()[0:n_components]

	# Modeling
	# Create simple KMeans model, and fit to earliest data set (2009).
	# Keeping centroid constant, predict clusters for subsequent years (2010-2014).
	# Return a dictionary of the predictions (one entry per year).

	print "Modeling: kMeans"
	km = KMeans(n_clusters=8)
	clus9 = km.fit_predict(pca_df[pca_df.year == 2009].sort('ogc_fid')[columns])
	clus10 = km.predict(pca_df[pca_df.year == 2010].sort('ogc_fid')[columns])
	clus11 = km.predict(pca_df[pca_df.year == 2011].sort('ogc_fid')[columns])
	clus12 = km.predict(pca_df[pca_df.year == 2012].sort('ogc_fid')[columns])
	clus13 = km.predict(pca_df[pca_df.year == 2013].sort('ogc_fid')[columns])
	clus14 = km.predict(pca_df[pca_df.year == 2014].sort('ogc_fid')[columns])

	# Aggregate yearly clusters into one dictionary.
	clus = {2009:clus9, 2010:clus10, 2011:clus11, 2012:clus12, 2013:clus13, 2014:clus14 }

	# Create dictionary of crime counts for display later
	# quality_count {'2009':q_count}
	crime_data = {}
	for year in [2009, 2010, 2011, 2012, 2013, 2014]:
		crime_data[year] = cdf[df_index.year == year]

	return clus, crime_data
