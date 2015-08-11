import psycopg2
import pandas as pd
from subprocess import call
from preprocessing_crime import pre_process
import os

def create_database(db_name='oakland'):

	try:
		conn = psycopg2.connect(dbname = 'postgres',
								user = 'danaezoule',
								host = 'localhost')
	except:
		conn = None
		print "I am unable to connect to the database."

	conn.autocommit = True
	cur = conn.cursor()
	cur.execute("DROP DATABASE IF EXISTS " + db_name + ";")
	cur.execute("CREATE DATABASE " + db_name + ";")
	cur.close()
	conn.close()

	conn = psycopg2.connect(dbname = 'oakland', user = 'danaezoule', host = 'localhost')
	cur = conn.cursor()
	cur.execute('''
				-- Enable PostGIS (includes raster)
				CREATE EXTENSION postgis;''')
	conn.commit()
	cur.close()
	conn.close()

def connect_database(db_name='oakland'):

	try:
		conn = psycopg2.connect(dbname = 'oakland',
								user = 'danaezoule',
								host = 'localhost')

	except:
		conn = None
		print "I am unable to connect to the database."

	return conn

def create_crime_table():
	'''
	Read in crime data from csv
	Preprocessing:
		change Date type to pandas datetime
		add Year column
		add Year_Month column
		drop text columns
		map CrimeCat to grouped Crime types (see crime_dict.py for details)
		create dummy variables for CrimeCat
	Create postgres table 
	Load data into postgres
	Add geom column
	Create geom points
	'''
	print "preprocessing data..."
	cdf = pd.read_csv("data/OPD_150308.csv")
	cdf = pre_process(cdf) # From preprocessing_crime.py
	
	cdf_dummy = pd.get_dummies(cdf, prefix='CTYPE', columns=['CrimeCat'])
	#cdf_dummy.dropna(inplace=True) nans now dropped in pre_process
	cdf_dummy.to_csv("data/cdf.csv")

	print "Creating crime table..."
	cur.execute('''
		DROP TABLE IF EXISTS crime;

		CREATE TABLE crime (Idx serial PRIMARY KEY,
							Date date,
							Time time,
							Lat float8,
							Lng float8,
							Year int,
							Year_Month int,
							CTYPE_DOM_VIOL float8,
							CTYPE_NONVIOLENT float8,
							CTYPE_OTHER float8,
							CTYPE_OTHER_MISSING_PERSON float8,
							CTYPE_OTHER_RUNAWAY float8,
							CTYPE_QUALITY float8,
							CTYPE_TRAFFIC_MISDEMEANOR float8,
							CTYPE_TRAFFIC_TOWED_VEHICLE float8,
							CTYPE_VEHICLE_BREAK_IN float8,
							CTYPE_VEHICLE_THEFT float8,
							CTYPE_VIOLENT float8,
							CTYPE_WARRANT float8
							);

		COPY crime FROM '/Users/danaezoule/Documents/oakland-crime-housing/data/cdf.csv' WITH DELIMITER ',' CSV HEADER;

		SELECT AddGeometryColumn ('crime', 'geom', 4326, 'POINT', 2);

		UPDATE crime SET geom = ST_SetSRID(ST_MakePoint(Lng, Lat), 4326);
	''')
	conn.commit()

def create_shape_table():
	'''
	Load shape files into database
	'''
	print "Creating shape table..."
	os.system("ogr2ogr -f 'PostgreSQL' PG:'dbname= oakland user=danaezoule' '/Users/danaezoule/Documents/oakland-crime-housing/tl_2014_06_tabblock10' -nlt PROMOTE_TO_MULTI -nln shp_table -append")
	cur.execute("SELECT UpdateGeometrySRID('shp_table', 'wkb_geometry', 4326);")
	conn.commit()

def join_crime_blocks():
	'''
	Assign census blocks to crime data
	'''
	pass

if __name__ == "__main__":
	print "Creating Database"
	create_database()
	print "Connecting to Database"
	conn = connect_database()

	if conn:
		cur = conn.cursor()
		print "Creating Crime Table"
		create_crime_table()
		create_shape_table()
		#join_crime_blocks()
