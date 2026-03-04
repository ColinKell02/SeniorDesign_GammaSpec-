# SeniorDesign_GammaSpec-
This reposistory will be the home for the senior design project. Involving the gamma spectroscopy of the Moon, Mars, and Ceres. In addition an API feature will be within the project but wont be worked on directly with the other three tasks. 

# NASA Data Fetcher Dependencies
None - uses only Python Standard Library

# NASA Data Plotter Dependencies
matplotlib,
numpy,
pds4_tools,
pandas,
plotly

#pytest Dependencies
pytest, 
pytest-mock

## LunarProspector_WebApp 

# Dependencies
pandas
numpy
plotly
dash
dash vtk
scipy
pds4_tools
PIL

# Functionality
This script creates a simple web app for viewing GRS data from the lunar prospector.
It provides options for either the ~100 km or ~30 km data.
Clicking a point on the moon's surface will automatically provide the spectrum for the closest cooridnate.
Heat maps are also available for the 100 km data for various elements using the derivied PDS elemental abundance data.

The web app can be accessed by crtl + clicking the link provided in the terminal on running the script or pasting the link into any web browser.
If the app lags or stutters, the resolution of the moon surface can be lowered in the script. The image is a 2k max image.