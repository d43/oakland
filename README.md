# OaklandHoods
<p>
## Description:
<p>
Describing change over time in similarity of Oakland neighborhoods using geographically tagged quantitative data (crime incidents) and US census shape files. Project techniques and tools include unsupervised learning, k-means clustering (sklearn), geospatial analysis (PostgreSQL/PostGIS), visualization (Google Maps API, Google Charts API, javascript).
<p>
### Preprocessing:
<p>
Files: database.py, crime_dict.py, preprocessing_crime.py
<p>
Create database/tables, pre-process crime data, load crime data into database, load shape files into database, create feature table grouped by area and year.
<p>
### Clustering:
<p>
Files: model.py
<p>
K-means model, fitted with first year's (2009's) data. Clusters of future years predicted with fitted model giving visibility into how neighborhoods move in and out of clusters over time.
<p>
### Visualization/Presentation:
<p>
Files: app.py, database_to_json.py, templates/oakland.html
<p>
Neighborhood clustering infographic.
<p>
Resources: ryd.io, ansonwhitmer.tumblr.com/post/76570597222/sf-hoods-project
