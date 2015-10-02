		DROP TABLE IF EXISTS area_features;

		CREATE TABLE area_features AS
				WITH temp AS (
					SELECT ogc_fid, COUNT(DISTINCT Year) as years
					FROM crime_blocks
					WHERE Year < 2015
					GROUP BY ogc_fid)
				WITH q AS (
					SELECT *
					FROM crime_blocks
					WHERE CTYPE_QUALITY = 1)
				WITH nv AS (
					SELECT *
					FROM crime_blocks
					WHERE CTYPE_NONVIOLENT = 1)
				WITH vbi AS (
					SELECT *
					FROM crime_blocks
					WHERE CTYPE_VEHICLE_BREAK_IN = 1)
				WITH vt AS (
					SELECT *
					FROM crime_blocks
					WHERE CTYPE_VEHICLE_THEFT = 1)
				WITH v AS (
					SELECT *
					FROM crime_blocks
					WHERE CTYPE_VIOLENT = 1)
				SELECT c.ogc_fid, c.Year,
					SUM(c.CTYPE_QUALITY) as q_count,
					SUM(c.CTYPE_NONVIOLENT) as nv_count,
					SUM(c.CTYPE_VEHICLE_BREAK_IN) as vbi_count,
					SUM(c.CTYPE_VEHICLE_THEFT) as vt_count,
					SUM(c.CTYPE_VIOLENT) as v_count,
					SUM(q.Weekend) as q_weekend,
					SUM(q.Morning) as q_morning,
					SUM(q.Workday) as q_workday,
					SUM(q.Evening) as q_evening,
					SUM(q.Hr_0) as q_hr0,				
					SUM(q.Hr_1) as q_hr1,	
					SUM(q.Hr_2) as q_hr2,	
					SUM(q.Hr_3) as q_hr3,	
					SUM(q.Hr_4) as q_hr4,	
					SUM(q.Hr_5) as q_hr5,	
					SUM(q.Hr_6) as q_hr6,	
					SUM(q.Hr_7) as q_hr7,	
					SUM(q.Hr_8) as q_hr8,	
					SUM(q.Hr_9) as q_hr9,	
					SUM(q.Hr_10) as q_hr10,	
					SUM(q.Hr_11) as q_hr11,	
					SUM(q.Hr_12) as q_hr12,	
					SUM(q.Hr_13) as q_hr13,	
					SUM(q.Hr_14) as q_hr14,	
					SUM(q.Hr_15) as q_hr15,	
					SUM(q.Hr_16) as q_hr16,	
					SUM(q.Hr_17) as q_hr17,	
					SUM(q.Hr_18) as q_hr18,	
					SUM(q.Hr_19) as q_hr19,	
					SUM(q.Hr_20) as q_hr20,	
					SUM(q.Hr_21) as q_hr21,	
					SUM(q.Hr_22) as q_hr22,	
					SUM(q.Hr_23) as q_hr23,	
					SUM(q.Dow_0) as q_dow0,
					SUM(q.Dow_1) as q_dow1,
					SUM(q.Dow_2) as q_dow2,
					SUM(q.Dow_3) as q_dow3,
					SUM(q.Dow_4) as q_dow4,
					SUM(q.Dow_5) as q_dow5,
					SUM(q.Dow_6) as q_dow6,
					SUM(nv.Weekend) as nv_weekend,
					SUM(nv.Morning) as nv_morning,
					SUM(nv.Workday) as nv_workday,
					SUM(nv.Evening) as nv_evening,
					SUM(nv.Hr_0) as nv_hr0,				
					SUM(nv.Hr_1) as nv_hr1,	
					SUM(nv.Hr_2) as nv_hr2,	
					SUM(nv.Hr_3) as nv_hr3,	
					SUM(nv.Hr_4) as nv_hr4,	
					SUM(nv.Hr_5) as nv_hr5,	
					SUM(nv.Hr_6) as nv_hr6,	
					SUM(nv.Hr_7) as nv_hr7,	
					SUM(nv.Hr_8) as nv_hr8,	
					SUM(nv.Hr_9) as nv_hr9,	
					SUM(nv.Hr_10) as nv_hr10,	
					SUM(nv.Hr_11) as nv_hr11,	
					SUM(nv.Hr_12) as nv_hr12,	
					SUM(nv.Hr_13) as nv_hr13,	
					SUM(nv.Hr_14) as nv_hr14,	
					SUM(nv.Hr_15) as nv_hr15,	
					SUM(nv.Hr_16) as nv_hr16,	
					SUM(nv.Hr_17) as nv_hr17,	
					SUM(nv.Hr_18) as nv_hr18,	
					SUM(nv.Hr_19) as nv_hr19,	
					SUM(nv.Hr_20) as nv_hr20,	
					SUM(nv.Hr_21) as nv_hr21,	
					SUM(nv.Hr_22) as nv_hr22,	
					SUM(nv.Hr_23) as nv_hr23,	
					SUM(nv.Dow_0) as nv_dow0,
					SUM(nv.Dow_1) as nv_dow1,
					SUM(nv.Dow_2) as nv_dow2,
					SUM(nv.Dow_3) as nv_dow3,
					SUM(nv.Dow_4) as nv_dow4,
					SUM(nv.Dow_5) as nv_dow5,
					SUM(nv.Dow_6) as nv_dow6,
					SUM(vbi.Weekend) as vbi_weekend,
					SUM(vbi.Morning) as vbi_morning,
					SUM(vbi.Workday) as vbi_workday,
					SUM(vbi.Evening) as vbi_evening,
					SUM(vbi.Hr_0) as vbi_hr0,				
					SUM(vbi.Hr_1) as vbi_hr1,	
					SUM(vbi.Hr_2) as vbi_hr2,	
					SUM(vbi.Hr_3) as vbi_hr3,	
					SUM(vbi.Hr_4) as vbi_hr4,	
					SUM(vbi.Hr_5) as vbi_hr5,	
					SUM(vbi.Hr_6) as vbi_hr6,	
					SUM(vbi.Hr_7) as vbi_hr7,	
					SUM(vbi.Hr_8) as vbi_hr8,	
					SUM(vbi.Hr_9) as vbi_hr9,	
					SUM(vbi.Hr_10) as vbi_hr10,	
					SUM(vbi.Hr_11) as vbi_hr11,	
					SUM(vbi.Hr_12) as vbi_hr12,	
					SUM(vbi.Hr_13) as vbi_hr13,	
					SUM(vbi.Hr_14) as vbi_hr14,	
					SUM(vbi.Hr_15) as vbi_hr15,	
					SUM(vbi.Hr_16) as vbi_hr16,	
					SUM(vbi.Hr_17) as vbi_hr17,	
					SUM(vbi.Hr_18) as vbi_hr18,	
					SUM(vbi.Hr_19) as vbi_hr19,	
					SUM(vbi.Hr_20) as vbi_hr20,	
					SUM(vbi.Hr_21) as vbi_hr21,	
					SUM(vbi.Hr_22) as vbi_hr22,	
					SUM(vbi.Hr_23) as vbi_hr23,	
					SUM(vbi.Dow_0) as vbi_dow0,
					SUM(vbi.Dow_1) as vbi_dow1,
					SUM(vbi.Dow_2) as vbi_dow2,
					SUM(vbi.Dow_3) as vbi_dow3,
					SUM(vbi.Dow_4) as vbi_dow4,
					SUM(vbi.Dow_5) as vbi_dow5,
					SUM(vbi.Dow_6) as vbi_dow6,
					SUM(vt.Weekend) as vt_weekend,
					SUM(vt.Morning) as vt_morning,
					SUM(vt.Workday) as vt_workday,
					SUM(vt.Evening) as vt_evening,
					SUM(vt.Hr_0) as vt_hr0,				
					SUM(vt.Hr_1) as vt_hr1,	
					SUM(vt.Hr_2) as vt_hr2,	
					SUM(vt.Hr_3) as vt_hr3,	
					SUM(vt.Hr_4) as vt_hr4,	
					SUM(vt.Hr_5) as vt_hr5,	
					SUM(vt.Hr_6) as vt_hr6,	
					SUM(vt.Hr_7) as vt_hr7,	
					SUM(vt.Hr_8) as vt_hr8,	
					SUM(vt.Hr_9) as vt_hr9,	
					SUM(vt.Hr_10) as vt_hr10,	
					SUM(vt.Hr_11) as vt_hr11,	
					SUM(vt.Hr_12) as vt_hr12,	
					SUM(vt.Hr_13) as vt_hr13,	
					SUM(vt.Hr_14) as vt_hr14,	
					SUM(vt.Hr_15) as vt_hr15,	
					SUM(vt.Hr_16) as vt_hr16,	
					SUM(vt.Hr_17) as vt_hr17,	
					SUM(vt.Hr_18) as vt_hr18,	
					SUM(vt.Hr_19) as vt_hr19,	
					SUM(vt.Hr_20) as vt_hr20,	
					SUM(vt.Hr_21) as vt_hr21,	
					SUM(vt.Hr_22) as vt_hr22,	
					SUM(vt.Hr_23) as vt_hr23,	
					SUM(vt.Dow_0) as vt_dow0,
					SUM(vt.Dow_1) as vt_dow1,
					SUM(vt.Dow_2) as vt_dow2,
					SUM(vt.Dow_3) as vt_dow3,
					SUM(vt.Dow_4) as vt_dow4,
					SUM(vt.Dow_5) as vt_dow5,
					SUM(vt.Dow_6) as vt_dow6,
					SUM(v.Weekend) as nv_weekend,
					SUM(v.Morning) as nv_morning,
					SUM(v.Workday) as nv_workday,
					SUM(v.Evening) as nv_evening,
					SUM(v.Hr_0) as v_hr0,				
					SUM(v.Hr_1) as v_hr1,	
					SUM(v.Hr_2) as v_hr2,	
					SUM(v.Hr_3) as v_hr3,	
					SUM(v.Hr_4) as v_hr4,	
					SUM(v.Hr_5) as v_hr5,	
					SUM(v.Hr_6) as v_hr6,	
					SUM(v.Hr_7) as v_hr7,	
					SUM(v.Hr_8) as v_hr8,	
					SUM(v.Hr_9) as v_hr9,	
					SUM(v.Hr_10) as v_hr10,	
					SUM(v.Hr_11) as v_hr11,	
					SUM(v.Hr_12) as v_hr12,	
					SUM(v.Hr_13) as v_hr13,	
					SUM(v.Hr_14) as v_hr14,	
					SUM(v.Hr_15) as v_hr15,	
					SUM(v.Hr_16) as v_hr16,	
					SUM(v.Hr_17) as v_hr17,	
					SUM(v.Hr_18) as v_hr18,	
					SUM(v.Hr_19) as v_hr19,	
					SUM(v.Hr_20) as v_hr20,	
					SUM(v.Hr_21) as v_hr21,	
					SUM(v.Hr_22) as v_hr22,	
					SUM(v.Hr_23) as v_hr23,	
					SUM(v.Dow_0) as v_dow0,
					SUM(v.Dow_1) as v_dow1,
					SUM(v.Dow_2) as v_dow2,
					SUM(v.Dow_3) as v_dow3,
					SUM(v.Dow_4) as v_dow4,
					SUM(v.Dow_5) as v_dow5,
					SUM(v.Dow_6) as v_dow6
				FROM crime_blocks AS c
				JOIN temp AS t
				ON c.ogc_fid = t.ogc_fid
				JOIN q
				ON c.ogc_fid = q.ogc_fid AND c.Year = q.Year
				JOIN nv
				ON c.ogc_fid = nv.ogc_fid AND c.Year = nv.Year
				JOIN vbi
				ON c.ogc_fid = vib.ogc_fid AND c.Year = vib.Year
				JOIN vt
				ON c.ogc_fid = vt.ogc_fid AND c.Year = vt.Year
				JOIN v
				ON c.ogc_fid = v.ogc_fid AND c.Year = v.Year
				WHERE c.Year < 2015
				AND t.years = 6
				GROUP BY c.ogc_fid, c.Year;


		CREATE TABLE area_features AS
				WITH temp AS (
					SELECT ogc_fid, COUNT(DISTINCT Year) as years
					FROM crime_blocks
					WHERE Year < 2015
					GROUP BY ogc_fid)
				WITH q AS (
					SELECT *
					FROM crime_blocks
					WHERE CTYPE_QUALITY = 1)
				SELECT c.ogc_fid, c.Year,
					SUM(c.CTYPE_QUALITY) as q_count,	
					SUM(q.Weekend) as q_weekend,
				FROM crime_blocks AS c
				JOIN temp AS t
				ON c.ogc_fid = t.ogc_fid
				JOIN q
				ON c.ogc_fid = q.ogc_fid AND c.Year = q.Year
				WHERE c.Year < 2015
				AND t.years = 6
				GROUP BY c.ogc_fid, c.Year
				LIMIT 10;



WITH q AS (SELECT ogc_fid, 
				  year, 
				  SUM(CTYPE_QUALITY) AS q_count,
				  SUM(Weekend) AS q_weekend,
				  SUM(Morning) as q_morning,
				  SUM(Workday) as q_workday,
				  SUM(Evening) as q_evening
		   FROM crime_blocks
		   WHERE CTYPE_QUALITY = 1
		   GROUP BY ogc_fid, Year)
	SELECT c.ogc_fid, c.Year,
			q.q_count
			q.q_weekend
			q.q_morning
			q.q_workday
			q.q_evening
	FROM crime_blocks
	JOIN q
	ON c.ogc_fid = q.ogc_fid AND c.Year = q.Year
	WHERE c.Year > 2014
	GROUP BY c.ogc_fid, c.Year
	ORDER BY c.ogc_fid
	LIMIT 10;

				FROM crime_blocks AS c
				JOIN temp AS t
				ON c.ogc_fid = t.ogc_fid
				JOIN q
				ON c.ogc_fid = q.ogc_fid AND c.Year = q.Year
				WHERE c.Year < 2015
				AND t.years = 6
				GROUP BY c.ogc_fid, c.Year
				LIMIT 10;
				 

	from crime_blocks 
	where ctype_quality = 1)


-- works:
select ogc_fid, year, sum(ctype_quality) as q_count, sum(weekend) as q_weekend from crime_Blocks where ctype_quality = 1 group by ogc_fid, year order by ogc_fid limit 10;



-- doesn't work:

with q as (select ogc_fid, 
	year, 
	sum(ctype_quality) as q_count, 
	sum(weekend) as q_weekend 
	from crime_blocks 
	where ctype_quality = 1 
	group by ogc_fid, year)
with v as (select ogc_fid, 
	year, 
	sum(ctype_violent) as v_count, 
	sum(weekend) as v_weekend 
	from crime_blocks 
	where ctype_violent = 1 
	group by ogc_fid, year)
select q.ogc_fid, q.Year, q.q_count, q.q_weekend, v.v_count, v.v_weekend
from q
join v
on q.ogc_fid = v.ogc_fid AND q.year = c.year 
where q.year > 2015 
order by q.ogc_fid 
limit 10;

with q as (select ogc_fid, year, sum(ctype_quality) as q_count, sum(weekend) as q_weekend from crime_blocks where ctype_quality = 1 group by ogc_fid, year) select c.ogc_fid, c.Year, q.q_count, q.q_weekend from crime_blocks c join q on c.ogc_fid = q.ogc_fid AND c.year = q.year where c.year < 2015 order by c.ogc_fid limit 10;


with q as (select ogc_fid, year, sum(ctype_quality) as q_count, sum(weekend) as q_weekend from crime_blocks where ctype_quality = 1 group by ogc_fid, year), v as (select ogc_fid, year, sum(ctype_violent) as v_count, sum(weekend) as v_weekend from crime_blocks where ctype_violent = 1 group by ogc_fid, year) select q.ogc_fid, q.Year, q.q_count, q.q_weekend, v.v_count, v.v_weekend from q join v on q.ogc_fid = v.ogc_fid AND q.year = v.year where q.year < 2015 order by q.ogc_fid limit 10;

