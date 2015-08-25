# oakland-crime-housing
<p>
## Description:
<p>
Describing change over time in similarity of Oakland neighborhoods using geographically tagged quantitative data (crime distributions, rental prices, housing prices) and US census shape files. Project techniques and tools include unsupervised learning, k-means clustering (sklearn), geospatial analysis (PostgreSQL/PostGIS), visualization.
<p>
## Preprocessing:
<p>
Create database/tables, pre-process crime data, load crime data into database, load shape files into database, group data by area. Files: database.py, crime_dict.py, preprocessing_crime.py
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
1. Combine maps of neighborhood clustering into infographic. Slider for time, hover over with crime distributions.
<p>
2. Flesh out tech summary for presentation.
<p>
### Extra:
<p>
6. Kriging, gaussian, other spatio correlation techniques.
<p>
Resources: ryd.io, ansonwhitmer.tumblr.com/post/76570597222/sf-hoods-project
