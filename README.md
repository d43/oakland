# oakland-crime-housing
<p>
## Description:
<p>
Describing change over time in similarity of Oakland neighborhoods using geographically tagged quantitative data (crime incidents) and US census shape files. Project techniques and tools include unsupervised learning, k-means clustering (sklearn), geospatial analysis (PostgreSQL/PostGIS), visualization (Google Maps API, javascript).
<p>
### Preprocessing:
<p>
Files: database.py, crime_dict.py, preprocessing_crime.py
<p>
Create database/tables, pre-process crime data, load crime data into database, load shape files into database, create feature tables grouped by area and year.
<p>
### Clustering:
<p>
Files: model.py
<p>
K-means model, fitted with first year's (2009's) data. Clusters of future years predicted with fitted model. To do: Feature engineering, PCA, model optimization. Consider changing geographic segment (block vs group block vs tract). Add rental, real estate, American Community Survey, or other additional data sets.
<p>
### Visualization/Presentation:
<p>
Files: app.py, database_to_json.py, templates/oakland.html
<p>
Neighborhood clustering infographic with slider for time. To do: Labels. Hover over to show crime distributions and definition of clusters/similarities. Flesh out tech summary for presentation.
<p>
### Extra:
<p>
6. Kriging, gaussian, other spatio correlation techniques.
<p>
Resources: ryd.io, ansonwhitmer.tumblr.com/post/76570597222/sf-hoods-project
