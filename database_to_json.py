import psycopg2
import pandas as pd
import json



def set_query():
	'''
	Input:
	- None

	Output:
	- Query string
	'''

	# Convert postGIS polygon to GeoJSON geometry.
	# Join on area_features to ensure that only areas with crime counts are included

	map_query = '''
				SELECT DISTINCT shp_table.ogc_fid as area_id,
				ST_AsGeoJSON(shp_table.wkb_geometry) as geom
				FROM shp_table
				JOIN area_features
				ON shp_table.ogc_fid = area_features.ogc_fid
				ORDER BY area_id
				;'''

	return map_query

def query_db(conn, query):
	'''
	Input:
	- Connection, Query

	Output:
	- Data
	'''
	cur = conn.cursor()
	cur.execute(query)
	df = pd.DataFrame(cur.fetchall())
	df.columns = ['area_id', 'geom']
	return df

def to_json(idx, row, clusters):
	'''
	Input:
	- Index of data frame, row of dataframe, dictionary of clusters (one entry per year)

	Output:
	- GeoJSON file
	'''
	color = ['#FFF703', '#1AFF00', '#00F7FF', '#0800FF', '#FF00EE', '#FFC300', '#A938FF', '#FF0000']

	# Create properties dictionary to assign color to location corresponding to yearly cluster.
	properties = {}
	
	for year, values in clusters.iteritems():
		properties[year] = color[values[idx]]
		year_qcount = str(year) + '_q_count'
		properties[year_q_count] = 

	# Create properties dictionary to store features for display in visualization.
	geo_json = {'type':'Feature', 'geometry':json.loads(row['geom']), 'properties':properties }

	return geo_json

def join_json(conn, clusters):
	'''
	Input:
	- Dictionary of clusters (from model.py)

	Output:
	- GeoJSON Feature Collection (ready for mapping)
	'''

	df = query_db(conn, set_query())
	geo_list = []

	for idx, row in df.iterrows():
		geo_list.append(to_json(idx, row, clusters))

	return json.dumps( { "type":"FeatureCollection", 
				"features":geo_list } )
