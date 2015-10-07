Old notes from model.py:

'''
Model To-Do:

Feature Engineering Brainstorming:
	Normalization:
	    Normalize by population: none, assume census divisions cover this
	    Normalize by geography (square footage or meterage)
	    Normalize by total crime count
	    Normalize across all features (dimensionality)

	Geographical:
	    Census tracts, group blocks, or blocks
	    	Group blocks?

	Time Group By:
	    Month, quarter, year
	    	Year!

	Time features:
	    Count for weekday or weekend
	    Count for time of day (morning, afternoon, eve, night)
	        Split by data. First hypothesis:
	        Morning: 6am-noon
	        Afternoon: noon-6pm
	        Eve: 6pm-midnight
	        Early: midnight-6am
    
	Housing etc:
	    Are Trulia neighborhoods census tracts? Can I get block group info from Trulia?
	    Will the ACS be helpful? Can I get yearly or quarterly ACS information?
	    
	Time component:
	    Create centroids from earliest data, map all points to same centroids
	    Create new centroids for each year (with varied data) as below 
	    
	Data that varies from year to year:
	    If I use it, can I detect similar centroids between years?
	    Should I instead ignore this completely, despite losing rental data?
	    
	Final model:
	    Run several times, pick best clusters- min of objective function (within cluster
	    variation, provided by pairwise squared Euclidean distances between observations)
	    Dendrogram/hierarchical clustering to pick k
'''