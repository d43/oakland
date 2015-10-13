# OakHoods
## Evolution of Oakland Neighborhoods
<p>
## Overview:
<p>
I wanted to add to the current discussion on Oakland gentrification by answering the smaller question: How has crime changed over Oakland, and how can that add to our understanding of Oakland neighborhoods?
<p>
I used unsupervised learning to identify five distinct types of Oakland neighborhoods:
<p>
Tranquil - Low crime areas, including the Oakland hills.  
Quiet Crime - Areas with only nonviolent larceny crimes and auto break ins.  
Transitional - Areas with high larceny, medium violence, and medium levels of quality crimes (such as noise complaints, graffiti, and loitering).  
Violent - Areas characterized by high assaults and quality crimes.  
Auto Break Ins - Areas with very large spikes in auto break ins.  
<p>
Website to be launched in early November, 2015!

## Process:
Crime incidents tagged with latitude/longitudes, timestamps, and crime categories were loaded into a postGres database with postGIS (geospatial capacity). I aggregated these incidents by US census block-group shape files and by year. My feature table had a row for each distinct area/year combination.  
Features included:  
Count of each crime type
Count of each crime type occuring on the weekend
Count of each crime type occuring in the morning, day, and evening
<p>
The area/year rows were normalized for all features. I looked at the variance across the set, and found that the count and distribution in time of quality crimes were the most descriptive of changes between neighborhoods, followed by violent crimes.  
The engineered features over weekend, morning, day, and evening did not appear to segment the data in a meaningful way. These features were kept for use in the data visualization.
<p>
I then passed 2009 data into kMeans (initialized with kmeans++) and compared the within group sum of squares for values of k from 1-12, which told me that five neighborhoods could be separated in the data (see types above). I re-initialized my kMeans for many random seeds and observed that the Tranquil, Violent, and Auto Break In neighborhoods were robust to intialization. The remaining two clusters changed between seeds. I picked a seed to define my last two clusters on their ability to be separated geographically.  
## Code Walk Through:
Preprocessing (/database):  
database.py creats the database and tables  
preprocessing_crime.py deduplicated the crime data and aggregates it into meta-crime categories by using the dictionary defined in crime_dict.py
<p>
Website and clustering (/app):  
app.py calls model.py to retrieve cluster and crime information and displays the informationvia templates/oakland.html  
oakland.html uses javascript, Google Maps API, and Google Charts API to display crime information for each year, for each area
model.py queries the database and retrieves the feature tables, scales the data (sklearn Standardscalar), performs kMeans (sklearn kMeans) with set random seed (203) and k=5 on 2009 feature rows, and maps future area/years to 2009 centroids.  The clusters and crime information is returned.
<p>
<p>
Resources: ryd.io, ansonwhitmer.tumblr.com/post/76570597222/sf-hoods-project
