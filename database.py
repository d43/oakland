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

def create_sub_features():
	'''
	Output: Five tables (one for each crime type: quality, nonviolent, vehicle_break_in,
	vehicle_theft) with one row for each area, for each year as follows:

	For area/year:
	_count: Count of all instances
	_weekend: Count of crimes occuring on the weekend 
	_morning: Count of crimes occuring in the morning (1-7am)
	_workday: Count of crimes occuring in the workday (8am - 3pm)
	_evening: Count of crimes occuring in the evening (4pm - 11pm)
	_hr0 to _hr 23: Count of crimes occuring at each hour of the day
	_dow0 to _dow6: Count of crimes occuring on each day of the week

	'''

	cur.execute('''
		DROP TABLE IF EXISTS q_features;

		CREATE TABLE q_features AS (
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
							group by ogc_fid, year);
		''')
	conn.commit()

	cur.execute('''

		DROP TABLE IF EXISTS nv_features;

		CREATE TABLE nv_features AS (
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
							group by ogc_fid, Year);

		''')
	conn.commit()

	cur.execute('''
		DROP TABLE IF EXISTS vbi_features;

		CREATE TABLE vbi_features AS (
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
							group by ogc_fid, Year);
		''')
	conn.commit()

	cur.execute('''

		DROP TABLE IF EXISTS vt_features;

		CREATE TABLE vt_features AS (
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
							group by ogc_fid, Year);
		''')
	conn.commit()

	cur.execute('''

		DROP TABLE IF EXISTS v_features;

		CREATE TABLE v_features AS (
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
							group by ogc_fid, Year);
		''')
	conn.commit()

def create_area_features():
	'''
	Output: Table with one row for each area, for each year, for each crime type as follows:

	For area/year/crime type (quality, nonviolent, vehicle_break_in, vehicle_theft):
	_count: Count of all crime_blocks
	_weekend: Count of crimes occuring on the weekend 
	_morning: Count of crimes occuring in the morning (1-7am)
	_workday: Count of crimes occuring in the workday (8am - 3pm)
	_evening: Count of crimes occuring in the evening (4pm - 11pm)
	_hr0 to _hr 23: Count of crimes occuring at each hour of the day
	_dow0 to _dow6: Count of crimes occuring on each day of the week

	Filters:
	Drop 2015 data for now (to add in if complete data received)
	Keep only areas with at least one crime per year for 2009-2014
	'''

	cur.execute('''

		DROP TABLE IF EXISTS area_features;

		CREATE TABLE area_features AS (
				WITH distinct_area_year AS (
						WITH temp AS (
							SELECT ogc_fid, COUNT(DISTINCT year) as years
							FROM crime_blocks
							WHERE year < 2015
							GROUP BY ogc_fid
							HAVING COUNT(DISTINCT year) = 6)
					SELECT c.ogc_fid, c.Year
					FROM crime_blocks c
					JOIN temp t
					ON c.ogc_fid = t.ogc_fid
					GROUP BY c.ogc_fid, c.year
					ORDER BY ogc_fid, year)
				SELECT d.ogc_fid,
					   d.Year,
					   COALESCE(q.q_count, 0) AS q_count,
					   COALESCE(q.q_weekend, 0) AS q_weekend,
					   COALESCE(q.q_morning, 0) AS q_morning,
					   COALESCE(q.q_workday, 0) AS q_workday,
					   COALESCE(q.q_evening, 0) AS q_evening,
					   COALESCE(q.q_hr0, 0) AS q_hr0,
					   COALESCE(q.q_hr1, 0) AS q_hr1,
					   COALESCE(q.q_hr2, 0) AS q_hr2,
					   COALESCE(q.q_hr3, 0) AS q_hr3,
					   COALESCE(q.q_hr4, 0) AS q_hr4,
					   COALESCE(q.q_hr5, 0) AS q_hr5,
					   COALESCE(q.q_hr6, 0) AS q_hr6,
					   COALESCE(q.q_hr7, 0) AS q_hr7,
					   COALESCE(q.q_hr8, 0) AS q_hr8,
					   COALESCE(q.q_hr9, 0) AS q_hr9,
					   COALESCE(q.q_hr10, 0) AS q_hr10,
					   COALESCE(q.q_hr11, 0) AS q_hr11,
					   COALESCE(q.q_hr12, 0) AS q_hr12,
					   COALESCE(q.q_hr13, 0) AS q_hr13,
					   COALESCE(q.q_hr14, 0) AS q_hr14,
					   COALESCE(q.q_hr15, 0) AS q_hr15,
					   COALESCE(q.q_hr16, 0) AS q_hr16,
					   COALESCE(q.q_hr17, 0) AS q_hr17,
					   COALESCE(q.q_hr18, 0) AS q_hr18,
					   COALESCE(q.q_hr19, 0) AS q_hr19,
					   COALESCE(q.q_hr20, 0) AS q_hr20,
					   COALESCE(q.q_hr21, 0) AS q_hr21,
					   COALESCE(q.q_hr22, 0) AS q_hr22,
					   COALESCE(q.q_hr23, 0) AS q_hr23,
					   COALESCE(q.q_dow0, 0) AS q_dow0,
					   COALESCE(q.q_dow1, 0) AS q_dow1,
					   COALESCE(q.q_dow2, 0) AS q_dow2,
					   COALESCE(q.q_dow3, 0) AS q_dow3,
					   COALESCE(q.q_dow4, 0) AS q_dow4,
					   COALESCE(q.q_dow5, 0) AS q_dow5,
					   COALESCE(q.q_dow6, 0) AS q_dow6,
					   COALESCE(nv.nv_count, 0) AS nv_count,
					   COALESCE(nv.nv_weekend, 0) AS nv_weekend,
					   COALESCE(nv.nv_morning, 0) AS nv_morning,
					   COALESCE(nv.nv_workday, 0) AS nv_workday,
					   COALESCE(nv.nv_evening, 0) AS nv_evening,
					   COALESCE(nv.nv_hr0, 0) AS nv_hr0,
					   COALESCE(nv.nv_hr1, 0) AS nv_hr1,
					   COALESCE(nv.nv_hr2, 0) AS nv_hr2,
					   COALESCE(nv.nv_hr3, 0) AS nv_hr3,
					   COALESCE(nv.nv_hr4, 0) AS nv_hr4,
					   COALESCE(nv.nv_hr5, 0) AS nv_hr5,
					   COALESCE(nv.nv_hr6, 0) AS nv_hr6,
					   COALESCE(nv.nv_hr7, 0) AS nv_hr7,
					   COALESCE(nv.nv_hr8, 0) AS nv_hr8,
					   COALESCE(nv.nv_hr9, 0) AS nv_hr9,
					   COALESCE(nv.nv_hr10, 0) AS nv_hr10,
					   COALESCE(nv.nv_hr11, 0) AS nv_hr11,
					   COALESCE(nv.nv_hr12, 0) AS nv_hr12,
					   COALESCE(nv.nv_hr13, 0) AS nv_hr13,
					   COALESCE(nv.nv_hr14, 0) AS nv_hr14,
					   COALESCE(nv.nv_hr15, 0) AS nv_hr15,
					   COALESCE(nv.nv_hr16, 0) AS nv_hr16,
					   COALESCE(nv.nv_hr17, 0) AS nv_hr17,
					   COALESCE(nv.nv_hr18, 0) AS nv_hr18,
					   COALESCE(nv.nv_hr19, 0) AS nv_hr19,
					   COALESCE(nv.nv_hr20, 0) AS nv_hr20,
					   COALESCE(nv.nv_hr21, 0) AS nv_hr21,
					   COALESCE(nv.nv_hr22, 0) AS nv_hr22,
					   COALESCE(nv.nv_hr23, 0) AS nv_hr23,
					   COALESCE(nv.nv_dow0, 0) AS nv_dow0,
					   COALESCE(nv.nv_dow1, 0) AS nv_dow1,
					   COALESCE(nv.nv_dow2, 0) AS nv_dow2,
					   COALESCE(nv.nv_dow3, 0) AS nv_dow3,
					   COALESCE(nv.nv_dow4, 0) AS nv_dow4,
					   COALESCE(nv.nv_dow5, 0) AS nv_dow5,
					   COALESCE(nv.nv_dow6, 0) AS nv_dow6,
					   COALESCE(vbi.vbi_count, 0) AS vbi_count,
					   COALESCE(vbi.vbi_weekend, 0) AS vbi_weekend,
					   COALESCE(vbi.vbi_morning, 0) AS vbi_morning,
					   COALESCE(vbi.vbi_workday, 0) AS vbi_workday,
					   COALESCE(vbi.vbi_evening, 0) AS vbi_evening,
					   COALESCE(vbi.vbi_hr0, 0) AS vbi_hr0,
					   COALESCE(vbi.vbi_hr1, 0) AS vbi_hr1,
					   COALESCE(vbi.vbi_hr2, 0) AS vbi_hr2,
					   COALESCE(vbi.vbi_hr3, 0) AS vbi_hr3,
					   COALESCE(vbi.vbi_hr4, 0) AS vbi_hr4,
					   COALESCE(vbi.vbi_hr5, 0) AS vbi_hr5,
					   COALESCE(vbi.vbi_hr6, 0) AS vbi_hr6,
					   COALESCE(vbi.vbi_hr7, 0) AS vbi_hr7,
					   COALESCE(vbi.vbi_hr8, 0) AS vbi_hr8,
					   COALESCE(vbi.vbi_hr9, 0) AS vbi_hr9,
					   COALESCE(vbi.vbi_hr10, 0) AS vbi_hr10,
					   COALESCE(vbi.vbi_hr11, 0) AS vbi_hr11,
					   COALESCE(vbi.vbi_hr12, 0) AS vbi_hr12,
					   COALESCE(vbi.vbi_hr13, 0) AS vbi_hr13,
					   COALESCE(vbi.vbi_hr14, 0) AS vbi_hr14,
					   COALESCE(vbi.vbi_hr15, 0) AS vbi_hr15,
					   COALESCE(vbi.vbi_hr16, 0) AS vbi_hr16,
					   COALESCE(vbi.vbi_hr17, 0) AS vbi_hr17,
					   COALESCE(vbi.vbi_hr18, 0) AS vbi_hr18,
					   COALESCE(vbi.vbi_hr19, 0) AS vbi_hr19,
					   COALESCE(vbi.vbi_hr20, 0) AS vbi_hr20,
					   COALESCE(vbi.vbi_hr21, 0) AS vbi_hr21,
					   COALESCE(vbi.vbi_hr22, 0) AS vbi_hr22,
					   COALESCE(vbi.vbi_hr23, 0) AS vbi_hr23,
					   COALESCE(vbi.vbi_dow0, 0) AS vbi_dow0,
					   COALESCE(vbi.vbi_dow1, 0) AS vbi_dow1,
					   COALESCE(vbi.vbi_dow2, 0) AS vbi_dow2,
					   COALESCE(vbi.vbi_dow3, 0) AS vbi_dow3,
					   COALESCE(vbi.vbi_dow4, 0) AS vbi_dow4,
					   COALESCE(vbi.vbi_dow5, 0) AS vbi_dow5,
					   COALESCE(vbi.vbi_dow6, 0) AS vbi_dow6,
					   COALESCE(vt.vt_count, 0) AS vt_count,
					   COALESCE(vt.vt_weekend, 0) AS vt_weekend,
					   COALESCE(vt.vt_morning, 0) AS vt_morning,
					   COALESCE(vt.vt_workday, 0) AS vt_workday,
					   COALESCE(vt.vt_evening, 0) AS vt_evening,
					   COALESCE(vt.vt_hr0, 0) AS vt_hr0,
					   COALESCE(vt.vt_hr1, 0) AS vt_hr1,
					   COALESCE(vt.vt_hr2, 0) AS vt_hr2,
					   COALESCE(vt.vt_hr3, 0) AS vt_hr3,
					   COALESCE(vt.vt_hr4, 0) AS vt_hr4,
					   COALESCE(vt.vt_hr5, 0) AS vt_hr5,
					   COALESCE(vt.vt_hr6, 0) AS vt_hr6,
					   COALESCE(vt.vt_hr7, 0) AS vt_hr7,
					   COALESCE(vt.vt_hr8, 0) AS vt_hr8,
					   COALESCE(vt.vt_hr9, 0) AS vt_hr9,
					   COALESCE(vt.vt_hr10, 0) AS vt_hr10,
					   COALESCE(vt.vt_hr11, 0) AS vt_hr11,
					   COALESCE(vt.vt_hr12, 0) AS vt_hr12,
					   COALESCE(vt.vt_hr13, 0) AS vt_hr13,
					   COALESCE(vt.vt_hr14, 0) AS vt_hr14,
					   COALESCE(vt.vt_hr15, 0) AS vt_hr15,
					   COALESCE(vt.vt_hr16, 0) AS vt_hr16,
					   COALESCE(vt.vt_hr17, 0) AS vt_hr17,
					   COALESCE(vt.vt_hr18, 0) AS vt_hr18,
					   COALESCE(vt.vt_hr19, 0) AS vt_hr19,
					   COALESCE(vt.vt_hr20, 0) AS vt_hr20,
					   COALESCE(vt.vt_hr21, 0) AS vt_hr21,
					   COALESCE(vt.vt_hr22, 0) AS vt_hr22,
					   COALESCE(vt.vt_hr23, 0) AS vt_hr23,
					   COALESCE(vt.vt_dow0, 0) AS vt_dow0,
					   COALESCE(vt.vt_dow1, 0) AS vt_dow1,
					   COALESCE(vt.vt_dow2, 0) AS vt_dow2,
					   COALESCE(vt.vt_dow3, 0) AS vt_dow3,
					   COALESCE(vt.vt_dow4, 0) AS vt_dow4,
					   COALESCE(vt.vt_dow5, 0) AS vt_dow5,
					   COALESCE(vt.vt_dow6, 0) AS vt_dow6,
					   COALESCE(v.v_count, 0) AS v_count,
					   COALESCE(v.v_weekend, 0) AS v_weekend,
					   COALESCE(v.v_morning, 0) AS v_morning,
					   COALESCE(v.v_workday, 0) AS v_workday,
					   COALESCE(v.v_evening, 0) AS v_evening,
					   COALESCE(v.v_hr0, 0) AS v_hr0,
					   COALESCE(v.v_hr1, 0) AS v_hr1,
					   COALESCE(v.v_hr2, 0) AS v_hr2,
					   COALESCE(v.v_hr3, 0) AS v_hr3,
					   COALESCE(v.v_hr4, 0) AS v_hr4,
					   COALESCE(v.v_hr5, 0) AS v_hr5,
					   COALESCE(v.v_hr6, 0) AS v_hr6,
					   COALESCE(v.v_hr7, 0) AS v_hr7,
					   COALESCE(v.v_hr8, 0) AS v_hr8,
					   COALESCE(v.v_hr9, 0) AS v_hr9,
					   COALESCE(v.v_hr10, 0) AS v_hr10,
					   COALESCE(v.v_hr11, 0) AS v_hr11,
					   COALESCE(v.v_hr12, 0) AS v_hr12,
					   COALESCE(v.v_hr13, 0) AS v_hr13,
					   COALESCE(v.v_hr14, 0) AS v_hr14,
					   COALESCE(v.v_hr15, 0) AS v_hr15,
					   COALESCE(v.v_hr16, 0) AS v_hr16,
					   COALESCE(v.v_hr17, 0) AS v_hr17,
					   COALESCE(v.v_hr18, 0) AS v_hr18,
					   COALESCE(v.v_hr19, 0) AS v_hr19,
					   COALESCE(v.v_hr20, 0) AS v_hr20,
					   COALESCE(v.v_hr21, 0) AS v_hr21,
					   COALESCE(v.v_hr22, 0) AS v_hr22,
					   COALESCE(v.v_hr23, 0) AS v_hr23,
					   COALESCE(v.v_dow0, 0) AS v_dow0,
					   COALESCE(v.v_dow1, 0) AS v_dow1,
					   COALESCE(v.v_dow2, 0) AS v_dow2,
					   COALESCE(v.v_dow3, 0) AS v_dow3,
					   COALESCE(v.v_dow4, 0) AS v_dow4,
					   COALESCE(v.v_dow5, 0) AS v_dow5,
					   COALESCE(v.v_dow6, 0) AS v_dow6
				FROM distinct_area_year AS d
				LEFT OUTER JOIN q_features q
				ON d.ogc_fid = q.ogc_fid AND d.Year = q.Year
				LEFT OUTER JOIN nv_features nv
				ON d.ogc_fid = nv.ogc_fid AND d.Year = nv.Year
				LEFT OUTER JOIN vbi_features vbi
				ON d.ogc_fid = vbi.ogc_fid AND d.Year = vbi.Year
				LEFT OUTER JOIN vt_features vt
				ON d.ogc_fid = vt.ogc_fid AND d.Year = vt.Year
				LEFT OUTER JOIN v_features v
				ON d.ogc_fid = v.ogc_fid AND d.Year = v.Year
				WHERE d.Year < 2015
				ORDER BY d.ogc_fid, d.Year);
		''')
	conn.commit()

	# Set all NaN's to zero



if __name__ == "__main__":
	#print "Creating Database"
	#create_database()
	print "Connecting to Database"
	conn = connect_database()

	if conn:
		cur = conn.cursor()
		#print "Creating Crime Table"
		#create_crime_table()
		#print "Loading Shapes"
		#create_shape_table()
		#print "Creating Crime Geom Table"
		#join_crime_blocks()
		print "Creating Features Tables"
		create_sub_features()
		print "Joining Feature Tables"
		create_area_features()
