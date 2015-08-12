# oakland-crime-housing
<p>
Create database/tables, pre-process crime data, load crime data into database, load shape files into database, group data by area.
<p>
## Next steps:
<p>
### Time Series:
<p>
1. Make time series plot function. Takes shape, time_segment. Displays 5 plots (one per crime level) for all past data, for the shape, as counted by the time_segment. 
<p>
2. Try the plot with both group block and census track. Pick one.
<p>
3. Define function to fit ARIMA model and predict against test set. Takes shape, time_segment.
<p>
4. Assess ability of each model to predict (residuals, ACF/PACF of residuals, adjust parameters of model, rinse and repeat ACF/PACF/adjust parameters until achieve stationary data).
<p>
5. Pick model (tract or group block, time level), optimize.
<p>
### Clustering:
<p>
1. Choose level of time resolution/perform clustering across time resolutions to find best.
<p>
2. Factor engineering
<p>
3. Model
<p>
4. Pick final time resolution, graph for each time segement
<p>
### Visualization/Presentation:
<p>
1. Combine map slider wow.
<p>
2. Follow ryd.io tech summary
<p>
3. choose area, show predicted crime levels
<p>
### Extra:
<p>
6. Check out kriging, other ways of spatio correlation. Gaussian etc.
<p>
<p>
Resources: ryd.io
