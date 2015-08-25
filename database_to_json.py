import psycopg2
import pandas as pd
import json

# Connect to database.

conn_dict = {'dbname':'oakland', 'user':'danaezoule', 'host':'/tmp'}
conn = psycopg2.connect(dbname=conn_dict['dbname'], user=conn_dict['user'], host=conn_dict['host'])
c = conn.cursor()

#add a step here to speed things up:
#check if file exists, if not, repull and pickle it
#see kevin's code: safe_walk_app/safe_walk_app.py save_geo_dict

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
				SELECT ST_AsGeoJSON(shp_table.wkb_geometry) as geom
				FROM shp_table
				JOIN area_features
				ON shp_table.ogc_fid = area_features.ogc_fid
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
	df.columns = ['geom']
	return df

def to_json(idx, row, clusters):
	'''
	Input:
	- Row of dataframe

	Output:
	- GeoJSON file
	'''
	color = ['#FF0000', '#FFF703', '#1AFF00', '#00F7FF', '#0800FF', '#FF00EE', '#FFC300', '#A938FF']

	# Create properties dictionary to assign colors based on each year
	properties = {}
	for year in clusters:
		properties[year] = clusters[year][idx]

	geo_json = {'type':'Feature', 'geometry':json.loads(row['geom']), 'properties':properties }
	#geo_json = {'type':'Feature', 'geometry':json.loads(row['geom']), 'properties':{ 'color':color[clusters[idx]] } }

	return geo_json

def join_json(clusters=None):
	'''
	Input:

	Output:
	- GeoJSON Feature Collection (ready for mapping)
	'''

	df = query_db(conn, set_query())
	geo_list = []

	for idx, row in df.iterrows():
		geo_list.append(to_json(idx, row, clusters))

	return json.dumps( { "type":"FeatureCollection", 
				"features":geo_list } )
