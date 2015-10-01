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
	cdf = pd.read_csv("data/OPD_150308.csv")
	pre_process(cdf) # From preprocessing_crime.py


	cur.execute('''
		DROP TABLE IF EXISTS crime;

		CREATE TABLE crime (Idx serial PRIMARY KEY,
							OPD_RD varchar,
							Date date,
							Time time,
							Lat float8,
							Lng float8,
							Year int,
							Weekend int,
							Morning int,
							Workday int,
							Evening int,
							CTYPE_QUALITY float8,
							CTYPE_NONVIOLENT float8,
							CTYPE_VEHICLE_BREAK_IN float8,
							CTYPE_VEHICLE_THEFT float8,
							CTYPE_VIOLENT float8
							Hr_0 float8,
							Hr_1 float8,
							Hr_2 float8,
							Hr_3 float8,
							Hr_4 float8,
							Hr_5 float8,
							Hr_6 float8,
							Hr_7 float8,
							Hr_8 float8,
							Hr_9 float8,
							Hr_10 float8,
							Hr_11 float8,
							Hr_12 float8,
							Hr_13 float8,
							Hr_14 float8,
							Hr_15 float8,
							Hr_16 float8,
							Hr_17 float8,
							Hr_18 float8,
							Hr_19 float8,
							Hr_20 float8,
							Hr_21 float8,
							Hr_22 float8,
							Hr_23 float8,
							Dow_0 float8,
							Dow_1 float8,
							Dow_2 float8,
							Dow_3 float8,
							Dow_4 float8,
							Dow_5 float8,
							Dow_6 float8
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
	os.system("ogr2ogr -f 'PostgreSQL' PG:'dbname= oakland user=danaezoule' '/Users/danaezoule/Documents/oakland-crime-housing/tl_2014_06_bg' -nlt PROMOTE_TO_MULTI -nln shp_table -append")
	cur.execute("SELECT UpdateGeometrySRID('shp_table', 'wkb_geometry', 4326);")
	conn.commit()

def join_crime_blocks():
	'''
	Assign census blocks to crime data in new table
	'''
	cur.execute('''
		DROP TABLE IF EXISTS crime_blocks;

		CREATE TABLE crime_blocks AS
				SELECT crime.*, shp_table.ogc_fid
				FROM crime JOIN shp_table
				ON ST_Within(crime.geom, shp_Table.wkb_geometry);
	''')
	conn.commit()

def create_area_features():
	'''
	Output: Table with one row for each area with following features:

	Year
	Crime 1 (Quality) Sum
	Crime 2 (Nonviolent) Sum
	Crime 3 (Car Break In) Sum
	Crime 4 (Car Theft) Sum
	Crime 5 (Violent) Sum

	Filters:
	Drop 2015 data for now (to add in if complete data received)
	Keep only areas with at least one crime per year for 2009-2014
	'''

	cur.execute('''
		DROP TABLE IF EXISTS area_features;

		CREATE TABLE area_features AS
				WITH temp AS (
					SELECT ogc_fid, COUNT(DISTINCT Year) as years
					FROM crime_blocks
					WHERE Year < 2015
					GROUP BY ogc_fid)
				SELECT c.ogc_fid, c.Year,
				SUM(c.CTYPE_QUALITY) as quality,
				SUM(c.CTYPE_NONVIOLENT) as nonviolent,
				SUM(c.CTYPE_VEHICLE_BREAK_IN) as vehicle_break_in,
				SUM(c.CTYPE_VEHICLE_THEFT) as vehicle_theft,
				SUM(c.CTYPE_VIOLENT) as violent
				FROM crime_blocks AS c
				JOIN temp AS t
				ON c.ogc_fid = t.ogc_fid
				WHERE c.Year < 2015
				AND t.years = 6
				GROUP BY c.ogc_fid, c.Year;

	''')
	conn.commit()


if __name__ == "__main__":
	print "Creating Database"
	create_database()
	print "Connecting to Database"
	conn = connect_database()

	if conn:
		cur = conn.cursor()
		print "Creating Crime Table"
		create_crime_table()
		print "Loading Shapes"
		create_shape_table()
		print "Creating Crime Geom Table"
		join_crime_blocks()
		print "Creating Feature Table"
		create_area_features()
