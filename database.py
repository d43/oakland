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
					SELECT ogc_fid, COUNT(DISTINCT Year) AS years
					FROM crime_blocks
					WHERE Year < 2015
					GROUP BY ogc_fid),
				q AS (
					SELECT  ogc_fid, 
						    Year, 
						    SUM(CTYPE_QUALITY) AS q_count,
						    SUM(Weekend) AS q_weekend,
							SUM(Morning) AS q_morning,
							SUM(Workday) AS q_workday,
							SUM(Evening) AS q_evening,
							SUM(Hr_0) AS q_hr0,				
							SUM(Hr_1) AS q_hr1,	
							SUM(Hr_2) AS q_hr2,	
							SUM(Hr_3) AS q_hr3,	
							SUM(Hr_4) AS q_hr4,	
							SUM(Hr_5) AS q_hr5,	
							SUM(Hr_6) AS q_hr6,	
							SUM(Hr_7) AS q_hr7,	
							SUM(Hr_8) AS q_hr8,	
							SUM(Hr_9) AS q_hr9,	
							SUM(Hr_10) AS q_hr10,	
							SUM(Hr_11) AS q_hr11,	
							SUM(Hr_12) AS q_hr12,	
							SUM(Hr_13) AS q_hr13,	
							SUM(Hr_14) AS q_hr14,	
							SUM(Hr_15) AS q_hr15,	
							SUM(Hr_16) AS q_hr16,	
							SUM(Hr_17) AS q_hr17,	
							SUM(Hr_18) AS q_hr18,	
							SUM(Hr_19) AS q_hr19,	
							SUM(Hr_20) AS q_hr20,	
							SUM(Hr_21) AS q_hr21,	
							SUM(Hr_22) AS q_hr22,	
							SUM(Hr_23) AS q_hr23,	
							SUM(Dow_0) AS q_dow0,
							SUM(Dow_1) AS q_dow1,
							SUM(Dow_2) AS q_dow2,
							SUM(Dow_3) AS q_dow3,
							SUM(Dow_4) AS q_dow4,
							SUM(Dow_5) AS q_dow5,
							SUM(Dow_6) AS q_dow6
							from crime_blocks 
							where ctype_quality = 1 
							group by ogc_fid, year),
				nv AS (
					SELECT  ogc_fid, 
						    Year, 
						    SUM(CTYPE_NONVIOLENT) AS nv_count,
						    SUM(Weekend) AS nv_weekend,
							SUM(Morning) AS nv_morning,
							SUM(Workday) AS nv_workday,
							SUM(Evening) AS nv_evening,
							SUM(Hr_0) AS nv_hr0,				
							SUM(Hr_1) AS nv_hr1,	
							SUM(Hr_2) AS nv_hr2,	
							SUM(Hr_3) AS nv_hr3,	
							SUM(Hr_4) AS nv_hr4,	
							SUM(Hr_5) AS nv_hr5,	
							SUM(Hr_6) AS nv_hr6,	
							SUM(Hr_7) AS nv_hr7,	
							SUM(Hr_8) AS nv_hr8,	
							SUM(Hr_9) AS nv_hr9,	
							SUM(Hr_10) AS nv_hr10,	
							SUM(Hr_11) AS nv_hr11,	
							SUM(Hr_12) AS nv_hr12,	
							SUM(Hr_13) AS nv_hr13,	
							SUM(Hr_14) AS nv_hr14,	
							SUM(Hr_15) AS nv_hr15,	
							SUM(Hr_16) AS nv_hr16,	
							SUM(Hr_17) AS nv_hr17,	
							SUM(Hr_18) AS nv_hr18,	
							SUM(Hr_19) AS nv_hr19,	
							SUM(Hr_20) AS nv_hr20,	
							SUM(Hr_21) AS nv_hr21,	
							SUM(Hr_22) AS nv_hr22,	
							SUM(Hr_23) AS nv_hr23,	
							SUM(Dow_0) AS nv_dow0,
							SUM(Dow_1) AS nv_dow1,
							SUM(Dow_2) AS nv_dow2,
							SUM(Dow_3) AS nv_dow3,
							SUM(Dow_4) AS nv_dow4,
							SUM(Dow_5) AS nv_dow5,
							SUM(Dow_6) AS nv_dow6
							from crime_blocks 
							where CTYPE_NONVIOLENT = 1 
							group by ogc_fid, Year),
				vbi AS (
					SELECT  ogc_fid, 
						    Year, 
						    SUM(CTYPE_VEHICLE_BREAK_IN) AS vbi_count,
						    SUM(Weekend) AS vbi_weekend,
							SUM(Morning) AS vbi_morning,
							SUM(Workday) AS vbi_workday,
							SUM(Evening) AS vbi_evening,
							SUM(Hr_0) AS vbi_hr0,				
							SUM(Hr_1) AS vbi_hr1,	
							SUM(Hr_2) AS vbi_hr2,	
							SUM(Hr_3) AS vbi_hr3,	
							SUM(Hr_4) AS vbi_hr4,	
							SUM(Hr_5) AS vbi_hr5,	
							SUM(Hr_6) AS vbi_hr6,	
							SUM(Hr_7) AS vbi_hr7,	
							SUM(Hr_8) AS vbi_hr8,	
							SUM(Hr_9) AS vbi_hr9,	
							SUM(Hr_10) AS vbi_hr10,	
							SUM(Hr_11) AS vbi_hr11,	
							SUM(Hr_12) AS vbi_hr12,	
							SUM(Hr_13) AS vbi_hr13,	
							SUM(Hr_14) AS vbi_hr14,	
							SUM(Hr_15) AS vbi_hr15,	
							SUM(Hr_16) AS vbi_hr16,	
							SUM(Hr_17) AS vbi_hr17,	
							SUM(Hr_18) AS vbi_hr18,	
							SUM(Hr_19) AS vbi_hr19,	
							SUM(Hr_20) AS vbi_hr20,	
							SUM(Hr_21) AS vbi_hr21,	
							SUM(Hr_22) AS vbi_hr22,	
							SUM(Hr_23) AS vbi_hr23,	
							SUM(Dow_0) AS vbi_dow0,
							SUM(Dow_1) AS vbi_dow1,
							SUM(Dow_2) AS vbi_dow2,
							SUM(Dow_3) AS vbi_dow3,
							SUM(Dow_4) AS vbi_dow4,
							SUM(Dow_5) AS vbi_dow5,
							SUM(Dow_6) AS vbi_dow6
							from crime_blocks 
							where CTYPE_VEHICLE_BREAK_IN = 1 
							group by ogc_fid, Year),
				vt AS (
					SELECT  ogc_fid, 
						    Year, 
						    SUM(CTYPE_VEHICLE_THEFT) AS vt_count,
						    SUM(Weekend) AS vt_weekend,
							SUM(Morning) AS vt_morning,
							SUM(Workday) AS vt_workday,
							SUM(Evening) AS vt_evening,
							SUM(Hr_0) AS vt_hr0,				
							SUM(Hr_1) AS vt_hr1,	
							SUM(Hr_2) AS vt_hr2,	
							SUM(Hr_3) AS vt_hr3,	
							SUM(Hr_4) AS vt_hr4,	
							SUM(Hr_5) AS vt_hr5,	
							SUM(Hr_6) AS vt_hr6,	
							SUM(Hr_7) AS vt_hr7,	
							SUM(Hr_8) AS vt_hr8,	
							SUM(Hr_9) AS vt_hr9,	
							SUM(Hr_10) AS vt_hr10,	
							SUM(Hr_11) AS vt_hr11,	
							SUM(Hr_12) AS vt_hr12,	
							SUM(Hr_13) AS vt_hr13,	
							SUM(Hr_14) AS vt_hr14,	
							SUM(Hr_15) AS vt_hr15,	
							SUM(Hr_16) AS vt_hr16,	
							SUM(Hr_17) AS vt_hr17,	
							SUM(Hr_18) AS vt_hr18,	
							SUM(Hr_19) AS vt_hr19,	
							SUM(Hr_20) AS vt_hr20,	
							SUM(Hr_21) AS vt_hr21,	
							SUM(Hr_22) AS vt_hr22,	
							SUM(Hr_23) AS vt_hr23,	
							SUM(Dow_0) AS vt_dow0,
							SUM(Dow_1) AS vt_dow1,
							SUM(Dow_2) AS vt_dow2,
							SUM(Dow_3) AS vt_dow3,
							SUM(Dow_4) AS vt_dow4,
							SUM(Dow_5) AS vt_dow5,
							SUM(Dow_6) AS vt_dow6
							from crime_blocks 
							where CTYPE_VEHICLE_THEFT = 1 
							group by ogc_fid, Year),
				v AS (
					SELECT  ogc_fid, 
						    Year, 
						    SUM(CTYPE_VIOLENT) AS v_count,
						    SUM(Weekend) AS v_weekend,
							SUM(Morning) AS v_morning,
							SUM(Workday) AS v_workday,
							SUM(Evening) AS v_evening,
							SUM(Hr_0) AS v_hr0,				
							SUM(Hr_1) AS v_hr1,	
							SUM(Hr_2) AS v_hr2,	
							SUM(Hr_3) AS v_hr3,	
							SUM(Hr_4) AS v_hr4,	
							SUM(Hr_5) AS v_hr5,	
							SUM(Hr_6) AS v_hr6,	
							SUM(Hr_7) AS v_hr7,	
							SUM(Hr_8) AS v_hr8,	
							SUM(Hr_9) AS v_hr9,	
							SUM(Hr_10) AS v_hr10,	
							SUM(Hr_11) AS v_hr11,	
							SUM(Hr_12) AS v_hr12,	
							SUM(Hr_13) AS v_hr13,	
							SUM(Hr_14) AS v_hr14,	
							SUM(Hr_15) AS v_hr15,	
							SUM(Hr_16) AS v_hr16,	
							SUM(Hr_17) AS v_hr17,	
							SUM(Hr_18) AS v_hr18,	
							SUM(Hr_19) AS v_hr19,	
							SUM(Hr_20) AS v_hr20,	
							SUM(Hr_21) AS v_hr21,	
							SUM(Hr_22) AS v_hr22,	
							SUM(Hr_23) AS v_hr23,	
							SUM(Dow_0) AS v_dow0,
							SUM(Dow_1) AS v_dow1,
							SUM(Dow_2) AS v_dow2,
							SUM(Dow_3) AS v_dow3,
							SUM(Dow_4) AS v_dow4,
							SUM(Dow_5) AS v_dow5,
							SUM(Dow_6) AS v_dow6
							from crime_blocks 
							where CTYPE_VIOLENT = 1 
							group by ogc_fid, Year)
				SELECT c.ogc_fid,
					   c.Year,
					   q.q_count,
					   q.q_weekend,
					   q.q_morning,
					   q.q_workday,
					   q.q_evening,
					   q.q_hr0,
					   q.q_hr1,
					   q.q_hr2,
					   q.q_hr3,					   
					   q.q_hr4,
					   q.q_hr5,
					   q.q_hr6,
					   q.q_hr7,					   
					   q.q_hr8,
					   q.q_hr9,					   
					   q.q_hr10,
					   q.q_hr11,
					   q.q_hr12,
					   q.q_hr13,
					   q.q_hr14,
					   q.q_hr15,
					   q.q_hr16,
					   q.q_hr17,
					   q.q_hr18,
					   q.q_hr19,
					   q.q_hr20,
					   q.q_hr21,
					   q.q_hr22,
					   q.q_hr23,
					   q.q_dow0,
					   q.q_dow1,
					   q.q_dow2,
					   q.q_dow3,
					   q.q_dow4,
					   q.q_dow5,
					   q.q_dow6,
					   nv.nv_count,
					   nv.nv_weekend,
					   nv.nv_morning,
					   nv.nv_workday,
					   nv.nv_evening,
					   nv.nv_hr0,
					   nv.nv_hr1,
					   nv.nv_hr2,
					   nv.nv_hr3,					   
					   nv.nv_hr4,
					   nv.nv_hr5,
					   nv.nv_hr6,
					   nv.nv_hr7,					   
					   nv.nv_hr8,
					   nv.nv_hr9,					   
					   nv.nv_hr10,
					   nv.nv_hr11,
					   nv.nv_hr12,
					   nv.nv_hr13,
					   nv.nv_hr14,
					   nv.nv_hr15,
					   nv.nv_hr16,
					   nv.nv_hr17,
					   nv.nv_hr18,
					   nv.nv_hr19,
					   nv.nv_hr20,
					   nv.nv_hr21,
					   nv.nv_hr22,
					   nv.nv_hr23,
					   nv.nv_dow0,
					   nv.nv_dow1,
					   nv.nv_dow2,
					   nv.nv_dow3,
					   nv.nv_dow4,
					   nv.nv_dow5,
					   nv.nv_dow6,
					   vbi.vbi_count,
					   vbi.vbi_weekend,
					   vbi.vbi_morning,
					   vbi.vbi_workday,
					   vbi.vbi_evening,
					   vbi.vbi_hr0,
					   vbi.vbi_hr1,
					   vbi.vbi_hr2,
					   vbi.vbi_hr3,					   
					   vbi.vbi_hr4,
					   vbi.vbi_hr5,
					   vbi.vbi_hr6,
					   vbi.vbi_hr7,					   
					   vbi.vbi_hr8,
					   vbi.vbi_hr9,					   
					   vbi.vbi_hr10,
					   vbi.vbi_hr11,
					   vbi.vbi_hr12,
					   vbi.vbi_hr13,
					   vbi.vbi_hr14,
					   vbi.vbi_hr15,
					   vbi.vbi_hr16,
					   vbi.vbi_hr17,
					   vbi.vbi_hr18,
					   vbi.vbi_hr19,
					   vbi.vbi_hr20,
					   vbi.vbi_hr21,
					   vbi.vbi_hr22,
					   vbi.vbi_hr23,
					   vbi.vbi_dow0,
					   vbi.vbi_dow1,
					   vbi.vbi_dow2,
					   vbi.vbi_dow3,
					   vbi.vbi_dow4,
					   vbi.vbi_dow5,
					   vbi.vbi_dow6,
					   vt.vt_count,
					   vt.vt_weekend,
					   vt.vt_morning,
					   vt.vt_workday,
					   vt.vt_evening,
					   vt.vt_hr0,
					   vt.vt_hr1,
					   vt.vt_hr2,
					   vt.vt_hr3,					   
					   vt.vt_hr4,
					   vt.vt_hr5,
					   vt.vt_hr6,
					   vt.vt_hr7,					   
					   vt.vt_hr8,
					   vt.vt_hr9,					   
					   vt.vt_hr10,
					   vt.vt_hr11,
					   vt.vt_hr12,
					   vt.vt_hr13,
					   vt.vt_hr14,
					   vt.vt_hr15,
					   vt.vt_hr16,
					   vt.vt_hr17,
					   vt.vt_hr18,
					   vt.vt_hr19,
					   vt.vt_hr20,
					   vt.vt_hr21,
					   vt.vt_hr22,
					   vt.vt_hr23,
					   vt.vt_dow0,
					   vt.vt_dow1,
					   vt.vt_dow2,
					   vt.vt_dow3,
					   vt.vt_dow4,
					   vt.vt_dow5,
					   vt.vt_dow6,
					   v.v_count,
					   v.v_weekend,
					   v.v_morning,
					   v.v_workday,
					   v.v_evening,
					   v.v_hr0,
					   v.v_hr1,
					   v.v_hr2,
					   v.v_hr3,					   
					   v.v_hr4,
					   v.v_hr5,
					   v.v_hr6,
					   v.v_hr7,					   
					   v.v_hr8,
					   v.v_hr9,					   
					   v.v_hr10,
					   v.v_hr11,
					   v.v_hr12,
					   v.v_hr13,
					   v.v_hr14,
					   v.v_hr15,
					   v.v_hr16,
					   v.v_hr17,
					   v.v_hr18,
					   v.v_hr19,
					   v.v_hr20,
					   v.v_hr21,
					   v.v_hr22,
					   v.v_hr23,
					   v.v_dow0,
					   v.v_dow1,
					   v.v_dow2,
					   v.v_dow3,
					   v.v_dow4,
					   v.v_dow5,
					   v.v_dow6
				FROM crime_blocks AS c
				JOIN temp AS t
				ON c.ogc_fid = t.ogc_fid
				JOIN q
				ON c.ogc_fid = q.ogc_fid AND c.Year = q.Year
				JOIN nv
				ON c.ogc_fid = nv.ogc_fid AND c.Year = nv.Year
				JOIN vbi
				ON c.ogc_fid = vbi.ogc_fid AND c.Year = vbi.Year
				JOIN vt
				ON c.ogc_fid = vt.ogc_fid AND c.Year = vt.Year
				JOIN v
				ON c.ogc_fid = v.ogc_fid AND c.Year = v.Year
				WHERE c.Year < 2015
				AND t.years = 6
				ORDER BY c.ogc_fid, c.Year;

	''')
	conn.commit()


if __name__ == "__main__":
	#print "Creating Database"
	#create_database()
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
