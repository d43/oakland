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
							CTYPE_VIOLENT float8,
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
				SUM(c.CTYPE_QUALITY) as q_count,
				SUM(c.CTYPE_NONVIOLENT) as nv_count,
				SUM(c.CTYPE_VEHICLE_BREAK_IN) as vbi_count,
				SUM(c.CTYPE_VEHICLE_THEFT) as vt_count,
				SUM(c.CTYPE_VIOLENT) as v_count,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Weekend = 2) THEN 1 ELSE 0 END) as q_weekend,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Morning = 2) THEN 1 ELSE 0 END) as q_morning,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Workday = 2) THEN 1 ELSE 0 END) as q_workday,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Evening = 2) THEN 1 ELSE 0 END) as q_evening,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_0 = 2) THEN 1 ELSE 0 END) as q_hr0,				
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_1 = 2) THEN 1 ELSE 0 END) as q_hr1,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_2 = 2) THEN 1 ELSE 0 END) as q_hr2,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_3 = 2) THEN 1 ELSE 0 END) as q_hr3,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_4 = 2) THEN 1 ELSE 0 END) as q_hr4,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_5 = 2) THEN 1 ELSE 0 END) as q_hr5,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_6 = 2) THEN 1 ELSE 0 END) as q_hr6,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_7 = 2) THEN 1 ELSE 0 END) as q_hr7,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_8 = 2) THEN 1 ELSE 0 END) as q_hr8,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_9 = 2) THEN 1 ELSE 0 END) as q_hr9,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_10 = 2) THEN 1 ELSE 0 END) as q_hr10,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_11 = 2) THEN 1 ELSE 0 END) as q_hr11,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_12 = 2) THEN 1 ELSE 0 END) as q_hr12,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_13 = 2) THEN 1 ELSE 0 END) as q_hr13,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_14 = 2) THEN 1 ELSE 0 END) as q_hr14,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_15 = 2) THEN 1 ELSE 0 END) as q_hr15,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_16 = 2) THEN 1 ELSE 0 END) as q_hr16,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_17 = 2) THEN 1 ELSE 0 END) as q_hr17,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_18 = 2) THEN 1 ELSE 0 END) as q_hr18,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_19 = 2) THEN 1 ELSE 0 END) as q_hr19,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_20 = 2) THEN 1 ELSE 0 END) as q_hr20,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_21 = 2) THEN 1 ELSE 0 END) as q_hr21,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_22 = 2) THEN 1 ELSE 0 END) as q_hr22,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Hr_23 = 2) THEN 1 ELSE 0 END) as q_hr23,	
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Dow_0 = 2) THEN 1 ELSE 0 END) as q_dow0,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Dow_1 = 2) THEN 1 ELSE 0 END) as q_dow1,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Dow_2 = 2) THEN 1 ELSE 0 END) as q_dow2,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Dow_3 = 2) THEN 1 ELSE 0 END) as q_dow3,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Dow_4 = 2) THEN 1 ELSE 0 END) as q_dow4,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Dow_5 = 2) THEN 1 ELSE 0 END) as q_dow5,
				SUM(CASE WHEN (c.CTYPE_QUALITY + c.Dow_6 = 2) THEN 1 ELSE 0 END) as q_dow6,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Weekend = 2) THEN 1 ELSE 0 END) as nv_weekend,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Morning = 2) THEN 1 ELSE 0 END) as nv_morning,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Workday = 2) THEN 1 ELSE 0 END) as nv_workday,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Evening = 2) THEN 1 ELSE 0 END) as nv_evening,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_0 = 2) THEN 1 ELSE 0 END) as nv_hr0,				
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_1 = 2) THEN 1 ELSE 0 END) as nv_hr1,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_2 = 2) THEN 1 ELSE 0 END) as nv_hr2,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_3 = 2) THEN 1 ELSE 0 END) as nv_hr3,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_4 = 2) THEN 1 ELSE 0 END) as nv_hr4,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_5 = 2) THEN 1 ELSE 0 END) as nv_hr5,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_6 = 2) THEN 1 ELSE 0 END) as nv_hr6,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_7 = 2) THEN 1 ELSE 0 END) as nv_hr7,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_8 = 2) THEN 1 ELSE 0 END) as nv_hr8,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_9 = 2) THEN 1 ELSE 0 END) as nv_hr9,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_10 = 2) THEN 1 ELSE 0 END) as nv_hr10,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_11 = 2) THEN 1 ELSE 0 END) as nv_hr11,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_12 = 2) THEN 1 ELSE 0 END) as nv_hr12,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_13 = 2) THEN 1 ELSE 0 END) as nv_hr13,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_14 = 2) THEN 1 ELSE 0 END) as nv_hr14,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_15 = 2) THEN 1 ELSE 0 END) as nv_hr15,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_16 = 2) THEN 1 ELSE 0 END) as nv_hr16,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_17 = 2) THEN 1 ELSE 0 END) as nv_hr17,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_18 = 2) THEN 1 ELSE 0 END) as nv_hr18,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_19 = 2) THEN 1 ELSE 0 END) as nv_hr19,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_20 = 2) THEN 1 ELSE 0 END) as nv_hr20,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_21 = 2) THEN 1 ELSE 0 END) as nv_hr21,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_22 = 2) THEN 1 ELSE 0 END) as nv_hr22,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Hr_23 = 2) THEN 1 ELSE 0 END) as nv_hr23,	
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Dow_0 = 2) THEN 1 ELSE 0 END) as nv_dow0,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Dow_1 = 2) THEN 1 ELSE 0 END) as nv_dow1,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Dow_2 = 2) THEN 1 ELSE 0 END) as nv_dow2,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Dow_3 = 2) THEN 1 ELSE 0 END) as nv_dow3,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Dow_4 = 2) THEN 1 ELSE 0 END) as nv_dow4,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Dow_5 = 2) THEN 1 ELSE 0 END) as nv_dow5,
				SUM(CASE WHEN (c.CTYPE_NONVIOLENT + c.Dow_6 = 2) THEN 1 ELSE 0 END) as nv_dow6,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Weekend = 2) THEN 1 ELSE 0 END) as vbi_weekend,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Morning = 2) THEN 1 ELSE 0 END) as vbi_morning,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Workday = 2) THEN 1 ELSE 0 END) as vbi_workday,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Evening = 2) THEN 1 ELSE 0 END) as vbi_evening,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_0 = 2) THEN 1 ELSE 0 END) as vbi_hr0,				
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_1 = 2) THEN 1 ELSE 0 END) as vbi_hr1,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_2 = 2) THEN 1 ELSE 0 END) as vbi_hr2,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_3 = 2) THEN 1 ELSE 0 END) as vbi_hr3,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_4 = 2) THEN 1 ELSE 0 END) as vbi_hr4,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_5 = 2) THEN 1 ELSE 0 END) as vbi_hr5,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_6 = 2) THEN 1 ELSE 0 END) as vbi_hr6,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_7 = 2) THEN 1 ELSE 0 END) as vbi_hr7,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_8 = 2) THEN 1 ELSE 0 END) as vbi_hr8,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_9 = 2) THEN 1 ELSE 0 END) as vbi_hr9,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_10 = 2) THEN 1 ELSE 0 END) as vbi_hr10,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_11 = 2) THEN 1 ELSE 0 END) as vbi_hr11,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_12 = 2) THEN 1 ELSE 0 END) as vbi_hr12,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_13 = 2) THEN 1 ELSE 0 END) as vbi_hr13,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_14 = 2) THEN 1 ELSE 0 END) as vbi_hr14,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_15 = 2) THEN 1 ELSE 0 END) as vbi_hr15,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_16 = 2) THEN 1 ELSE 0 END) as vbi_hr16,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_17 = 2) THEN 1 ELSE 0 END) as vbi_hr17,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_18 = 2) THEN 1 ELSE 0 END) as vbi_hr18,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_19 = 2) THEN 1 ELSE 0 END) as vbi_hr19,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_20 = 2) THEN 1 ELSE 0 END) as vbi_hr20,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_21 = 2) THEN 1 ELSE 0 END) as vbi_hr21,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_22 = 2) THEN 1 ELSE 0 END) as vbi_hr22,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Hr_23 = 2) THEN 1 ELSE 0 END) as vbi_hr23,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Dow_0 = 2) THEN 1 ELSE 0 END) as vbi_dow0,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Dow_1 = 2) THEN 1 ELSE 0 END) as vbi_dow1,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Dow_2 = 2) THEN 1 ELSE 0 END) as vbi_dow2,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Dow_3 = 2) THEN 1 ELSE 0 END) as vbi_dow3,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Dow_4 = 2) THEN 1 ELSE 0 END) as vbi_dow4,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Dow_5 = 2) THEN 1 ELSE 0 END) as vbi_dow5,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_BREAK_IN + c.Dow_6 = 2) THEN 1 ELSE 0 END) as vbi_dow6,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Weekend = 2) THEN 1 ELSE 0 END) as vt_weekend,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Morning = 2) THEN 1 ELSE 0 END) as vt_morning,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Workday = 2) THEN 1 ELSE 0 END) as vt_workday,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Evening = 2) THEN 1 ELSE 0 END) as vt_evening,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_0 = 2) THEN 1 ELSE 0 END) as vt_hr0,				
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_1 = 2) THEN 1 ELSE 0 END) as vt_hr1,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_2 = 2) THEN 1 ELSE 0 END) as vt_hr2,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_3 = 2) THEN 1 ELSE 0 END) as vt_hr3,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_4 = 2) THEN 1 ELSE 0 END) as vt_hr4,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_5 = 2) THEN 1 ELSE 0 END) as vt_hr5,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_6 = 2) THEN 1 ELSE 0 END) as vt_hr6,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_7 = 2) THEN 1 ELSE 0 END) as vt_hr7,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_8 = 2) THEN 1 ELSE 0 END) as vt_hr8,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_9 = 2) THEN 1 ELSE 0 END) as vt_hr9,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_10 = 2) THEN 1 ELSE 0 END) as vt_hr10,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_11 = 2) THEN 1 ELSE 0 END) as vt_hr11,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_12 = 2) THEN 1 ELSE 0 END) as vt_hr12,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_13 = 2) THEN 1 ELSE 0 END) as vt_hr13,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_14 = 2) THEN 1 ELSE 0 END) as vt_hr14,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_15 = 2) THEN 1 ELSE 0 END) as vt_hr15,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_16 = 2) THEN 1 ELSE 0 END) as vt_hr16,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_17 = 2) THEN 1 ELSE 0 END) as vt_hr17,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_18 = 2) THEN 1 ELSE 0 END) as vt_hr18,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_19 = 2) THEN 1 ELSE 0 END) as vt_hr19,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_20 = 2) THEN 1 ELSE 0 END) as vt_hr20,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_21 = 2) THEN 1 ELSE 0 END) as vt_hr21,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_22 = 2) THEN 1 ELSE 0 END) as vt_hr22,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Hr_23 = 2) THEN 1 ELSE 0 END) as vt_hr23,	
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Dow_0 = 2) THEN 1 ELSE 0 END) as vt_dow0,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Dow_1 = 2) THEN 1 ELSE 0 END) as vt_dow1,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Dow_2 = 2) THEN 1 ELSE 0 END) as vt_dow2,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Dow_3 = 2) THEN 1 ELSE 0 END) as vt_dow3,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Dow_4 = 2) THEN 1 ELSE 0 END) as vt_dow4,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Dow_5 = 2) THEN 1 ELSE 0 END) as vt_dow5,
				SUM(CASE WHEN (c.CTYPE_VEHICLE_THEFT + c.Dow_6 = 2) THEN 1 ELSE 0 END) as vt_dow6,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Weekend = 2) THEN 1 ELSE 0 END) as nv_weekend,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Morning = 2) THEN 1 ELSE 0 END) as nv_morning,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Workday = 2) THEN 1 ELSE 0 END) as nv_workday,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Evening = 2) THEN 1 ELSE 0 END) as nv_evening,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_0 = 2) THEN 1 ELSE 0 END) as v_hr0,				
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_1 = 2) THEN 1 ELSE 0 END) as v_hr1,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_2 = 2) THEN 1 ELSE 0 END) as v_hr2,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_3 = 2) THEN 1 ELSE 0 END) as v_hr3,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_4 = 2) THEN 1 ELSE 0 END) as v_hr4,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_5 = 2) THEN 1 ELSE 0 END) as v_hr5,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_6 = 2) THEN 1 ELSE 0 END) as v_hr6,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_7 = 2) THEN 1 ELSE 0 END) as v_hr7,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_8 = 2) THEN 1 ELSE 0 END) as v_hr8,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_9 = 2) THEN 1 ELSE 0 END) as v_hr9,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_10 = 2) THEN 1 ELSE 0 END) as v_hr10,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_11 = 2) THEN 1 ELSE 0 END) as v_hr11,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_12 = 2) THEN 1 ELSE 0 END) as v_hr12,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_13 = 2) THEN 1 ELSE 0 END) as v_hr13,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_14 = 2) THEN 1 ELSE 0 END) as v_hr14,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_15 = 2) THEN 1 ELSE 0 END) as v_hr15,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_16 = 2) THEN 1 ELSE 0 END) as v_hr16,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_17 = 2) THEN 1 ELSE 0 END) as v_hr17,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_18 = 2) THEN 1 ELSE 0 END) as v_hr18,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_19 = 2) THEN 1 ELSE 0 END) as v_hr19,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_20 = 2) THEN 1 ELSE 0 END) as v_hr20,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_21 = 2) THEN 1 ELSE 0 END) as v_hr21,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_22 = 2) THEN 1 ELSE 0 END) as v_hr22,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Hr_23 = 2) THEN 1 ELSE 0 END) as v_hr23,	
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Dow_0 = 2) THEN 1 ELSE 0 END) as v_dow0,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Dow_1 = 2) THEN 1 ELSE 0 END) as v_dow1,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Dow_2 = 2) THEN 1 ELSE 0 END) as v_dow2,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Dow_3 = 2) THEN 1 ELSE 0 END) as v_dow3,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Dow_4 = 2) THEN 1 ELSE 0 END) as v_dow4,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Dow_5 = 2) THEN 1 ELSE 0 END) as v_dow5,
				SUM(CASE WHEN (c.CTYPE_VIOLENT + c.Dow_6 = 2) THEN 1 ELSE 0 END) as v_dow6
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
