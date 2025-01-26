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

def check_pct_null(image, aoi, crs, crs_transform):
    # Get the internal mask of the image and invert so nodata is 1.
    nodata_mask = image.mask().Not()

    # Get the number of non-null pixels with .count().
    total_pixels = image.reduceRegion(
        reducer=ee.Reducer.count(), 
        geometry=aoi, 
        crs=crs,
        crsTransform = crs_transform, 
        maxPixels=1e8).get('label').getInfo()

    # If no valid pixels, return 100% nodata.
    if total_pixels is None:
        return 100.0  

    # Get the number of nodata pixels from the mask.
    nodata_pixels = nodata_mask.reduceRegion(
        reducer=ee.Reducer.sum(), 
        geometry=aoi, 
        crs=crs,
        crsTransform = crs_transform, 
        maxPixels=1e8).get('label').getInfo()

    # Calculate the percentage of nodata pixels.
    pct_nodata = (nodata_pixels / (total_pixels + nodata_pixels)) * 100

    return pct_nodata

def fetch_dynamic_world(aoi_path, start_date, end_date, out_dir):
    """
    Function to acquire Dynamic World rasters for a specific polygon and date range.
    """
    # Set both the start and end date to the date given (returning daily composites).
    start_date = ee.Date(start_date)
    end_date = ee.Date(end_date)
    
    # Get aoi boundaries.
    aoi = get_boundaries(aoi_path)
    
    # Load the Dynamic World image collection for the aoi and dates of interest.
    imcol = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
             .filterBounds(aoi)
             .filterDate(start_date, end_date)
            )
    
    # Extract CRS and CRS Transform from original raw data
    projection = imcol.first().projection().getInfo()
    crs = projection['crs']
    crs_transform = projection['transform']
    print(f"CRS: {crs}, CRS Transform: {crs_transform}")

    # Get the list of dates in the collection
    dates = imcol.aggregate_array('system:time_start').getInfo()
    np_dates = [np.datetime64(ee.Date(date).format('YYYY-MM-dd').getInfo()) for date in dates]
    unique_dates = np.unique(np_dates)
    print(f'Dynamic World Data Dates: {unique_dates}')

    tasks = []  # List to store export tasks

    for date in unique_dates:
    
        # Set start date (inclusive) and end date (exclusive) to get only images from one day.
        date_str = str(date)
        ee_start_date = ee.Date(date_str)
        ee_end_date = ee_start_date.advance(1, 'day')

        print(f'Computing composite for date: {date_str}')

        # Filter the image collection to the start and end dates.
        date_col = imcol.filterDate(ee_start_date, ee_end_date)

        # Calculate the number of images for each date.
        num_date_images = date_col.size().getInfo()
        print(f'Num of images for date: {num_date_images}')

        # For label band, reduce to the most probable land cover type (using mode reducer).
        dw_label_composite = date_col.select('label').reduce(ee.Reducer.mode()).toFloat()

        # For other bands, calculate mean probability across all pixels.        
        lc_bands = ['water', 'trees', 'grass', 'flooded_vegetation', 
                    'crops', 'shrub_and_scrub', 'built', 'bare', 'snow_and_ice']
        dw_bands_composite = date_col.select(lc_bands).reduce(ee.Reducer.mean()).toFloat()

        # Combine the label band to the other class bands.
        dw_composite = dw_bands_composite.addBands(dw_label_composite)

        # Rename the bands to remove "_mean" and "_mode" suffixes.
        new_band_names = lc_bands + ['label']  # Original names for lc_bands + label
        dw_composite = dw_composite.rename(new_band_names)
        
        # Check the percentage of nodata pixels.
        pct_nodata = check_pct_null(dw_composite, aoi, crs, crs_transform)
        print(f"Percentage of nodata pixels: {pct_nodata}%")

        # If the image is entirely nodata (i.e. the bit of the scene overlapping AOI)
        if pct_nodata == 100:
            print(f"Composite for {date_str} contains only nodata. Skipping...")
            continue

        # Construct output file path.
        file_name = f'composite_{date_str}'
        
        # Export the dynamic world data as a GeoTIFF.
        task = ee.batch.Export.image.toDrive(dw_composite, 
                                            description = file_name,
                                            folder = out_dir,
                                            region = aoi,
                                            crs = crs,
                                            crsTransform = crs_transform,
                                            maxPixels = 1e13, # Maximum number of pixels to export
                                            fileFormat = 'GeoTIFF',
                                            formatOptions = {'cloudOptimized': True})
        
        tasks.append(task)
        task.start()
        print(f"Export task started for {date_str}")

    return tasks

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

    start_date = config['start-date']
    end_date = config['end-date']
    out_dir = config['out-dir']
    aoi_path = config['aoi-path']

    # Call function to export dynamic world raster.
    tasks = fetch_dynamic_world(aoi_path, start_date, end_date, out_dir)

    for task in tasks:
        status = task.status()
        print(f"Task {status['description']} is {status['state']}")


