# General imports:
import numpy as np
import rioxarray as rxr
import xarray as xr
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import mapping
import cartopy.crs as ccrs 
import os
import yaml

# Google Earth Engine imports:
import ee
import geemap
from google.auth import default
import certifi
import ssl

def get_boundaries(path):
    """Given path to geojson, return AOI as an ee Geometry object."""

    # Load the geojson.
    aoi_file = gpd.read_file(path)
    
    # Convert the GeoPandas geometry to GeoJSON.
    aoi_geom = aoi_file.iloc[0].geometry.__geo_interface__
    
    # Convert to an ee Geometry object.
    ee_polygon = ee.Geometry(aoi_geom)
        
    return(ee_polygon)

def get_bbox(path):
    """Given path to geojson, return AOI bounding box."""

    # Load the geojson.
    aoi_file = gpd.read_file(path)

    # Calculate the bounding box [minx, miny, maxx, maxy].
    bounds = aoi_file.total_bounds 
    
    # Create an ee.Geometry object from the bounding box.
    aoi = ee.Geometry.Rectangle([bounds[0], bounds[1], bounds[2], bounds[3]])
    
    return(aoi)

def check_pct_null(image, aoi):
    # Get the internal mask of the image and invert so nodata is 1.
    nodata_mask = image.mask().Not()

    # Get the number of non-null pixels with .count().
    total_pixels = image.reduceRegion(
        reducer=ee.Reducer.count(), geometry=aoi, scale=10, maxPixels=1e8
    ).get('label_mode').getInfo()

    # Get the number of nodata pixels from the mask.
    nodata_pixels = nodata_mask.reduceRegion(
        reducer=ee.Reducer.sum(), geometry=aoi, scale=10, maxPixels=1e8
    ).get('label_mode').getInfo()

    # Calculate the percentage of nodata pixels.
    pct_nodata = (nodata_pixels / (total_pixels + nodata_pixels)) * 100

    return pct_nodata

def fetch_dynamic_world(aoi_path, date, date_buffer, nodata_threshold, out_dir):
    """
    Function to acquire Dynamic World raster for a specific polygon and date range.
    
    Inputs:
    aoi         : ee.Geometry.Polygon defining the area of interest
    date        : specific date to aim for (YYYY-MM-DD)
    date_buffer : number of days to buffer date parameter with on either side
                  (e.g. start_date = date - date_buffer & 
                   end_date = date + date_buffer)
    
    Outputs:
    xr_ds : xarray Dataset containing the Dynamic World data composited over time period.
    """
    # Use date_buffer to set the start and end date surrounding date of interest.
    start_date = ee.Date(date).advance(-date_buffer, 'day')
    end_date = ee.Date(date).advance(date_buffer, 'day')
    
    # Get aoi bounding box polygon.
    aoi = get_bbox(aoi_path)
    
    # Load the Dynamic World image collection for the aoi and dates of interest.
    imcol = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
             .filterBounds(aoi)
             .filterDate(start_date, end_date)
             .select('label') # 'Label' contains the band index of highest probability land cover class for each pixel.
            )

    # Get the list of dates in the collection
    dates = imcol.aggregate_array('system:time_start').getInfo()
    print(f'Full Dates: {dates}')
    dates = [np.datetime64(ee.Date(date).format('YYYY-MM-dd').getInfo()) for date in dates]
    print(f'Dynamic World Data Dates: {dates}')

    # If there is no data available, increase the date_buffer and re-run function.
    if len(dates) < 1:
        # If date_buffer exceeds 180, stop the recursion.
        if date_buffer >= 180:
            raise Exception(f"No Dynamic World data found within {date_buffer} days of the target date. Stopping recursion.")
        
        date_buffer += 15
        print(f"No dynamic world data is available within this time period. Increasing date buffer to: {date_buffer} and re-running.")

        # Re-run the function recursively with new date_buffer.
        return fetch_dynamic_world(aoi_path, date, date_buffer, nodata_threshold, out_dir)

    # Reduce to the most probable land cover type and calculate percent nodata.
    dw_composite = imcol.reduce(ee.Reducer.mode())
    pct_nodata = check_pct_null(dw_composite, aoi)
    
    # If the percent nodata is greater than the threshold, increase data_buffer and re-run function.
    if pct_nodata > nodata_threshold:
        # If date_buffer exceeds 180, stop the recursion.
        if date_buffer >= 180:
            raise Exception(f"Not enough Dynamic World data found within {date_buffer} days of the target date. Stopping recursion.")
        
        date_buffer += 15
        print(f"The amount of nodata in the image was: {pct_nodata}. Increasing date buffer to: {date_buffer} and re-running.")

        # Re-run the function recursively with new date_buffer.
        return fetch_dynamic_world(aoi_path, date, date_buffer, nodata_threshold, out_dir)
    
    # Otherwise, if there is enough data, export the composite.
    else:
        # Convert ee.Date to human-readable strings.
        start_date_str = start_date.format('YYYYMMdd').getInfo() 
        end_date_str = end_date.format('YYYYMMdd').getInfo()

        # Construct output file path.
        out_path = os.path.join(out_dir, f'dynamic_world_{start_date_str}_{end_date_str}.tif')
        
        # Export the dynamic world data as a GeoTIFF.
        geemap.ee_export_image(dw_composite, filename = out_path, scale = 10, region = aoi)
        print(f"Exported dynamic world composite to {out_path}")
        
        return out_path


if __name__ == "__main__":
    # Create a context using the certifi bundle.
    context = ssl.create_default_context(cafile=certifi.where())

    # Initialize earth engine and geemap.
    ee.Authenticate()
    ee.Initialize(opt_url="https://earthengine.googleapis.com", project="dynamic-world-pipeline")
    geemap.ee_initialize()

    # Read config information from yml file.
    with open("config.yml", "r") as yml:
        config = yaml.safe_load(yml)

    date = config['target-date']
    date_buffer = config['date-buffer']
    nodata_threshold = config['nodata-threshold']
    out_dir = config['out-dir']
    aoi_path = config['aoi-path']

    # Call function to export dynamic world raster.
    dynamic_world_tif = fetch_dynamic_world(aoi_path, date, date_buffer, nodata_threshold, out_dir)


