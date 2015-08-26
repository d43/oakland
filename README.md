# oakland-crime-housing
<p>
## Description:
<p>
Describing change over time in similarity of Oakland neighborhoods using geographically tagged quantitative data (crime incidents) and US census shape files. Project techniques and tools include unsupervised learning, k-means clustering (sklearn), geospatial analysis (PostgreSQL/PostGIS), visualization (Google Maps API, javascript).
<p>
## Preprocessing:
<p>
Create database/tables, pre-process crime data, load crime data into database, load shape files into database, create feature tables grouped by area and year. Files: database.py, crime_dict.py, preprocessing_crime.py
<p>
## Next steps:
<p>
### Clustering:
<p>
1. Choose level of time resolution/perform clustering across time resolutions to find best (picked year). Choose geographic resolution: census tract or group_block (picked group_block).
<p>
2. Factor engineering
<p>
3. Model. Files: model.py
<p>
4. Pick final time resolution, graph for each time segment
<p>
5. Add rental, real estate, American Community Survey, or other additional data sets.
<p>
### Visualization/Presentation:
<p>
1. Neighborhood clustering infographic with slider for time. In progress: labels, hover over to show crime distributions. Files: app.py, database_to_json.py, templates/oakland.html
<p>
2. Flesh out tech summary for presentation.
<p>
### Extra:
<p>
6. Kriging, gaussian, other spatio correlation techniques.
<p>
Resources: ryd.io, ansonwhitmer.tumblr.com/post/76570597222/sf-hoods-project
