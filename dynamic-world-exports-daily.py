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

    # If no valid pixels, return 100% nodata.
    if total_pixels is None:
        return 100.0  

    # Get the number of nodata pixels from the mask.
    nodata_pixels = nodata_mask.reduceRegion(
        reducer=ee.Reducer.sum(), geometry=aoi, scale=10, maxPixels=1e8
    ).get('label_mode').getInfo()

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
    
    # Get aoi bounding box polygon.
    #aoi = get_bbox(aoi_path)
    aoi = get_boundaries(aoi_path)
    
    # Load the Dynamic World image collection for the aoi and dates of interest.
    imcol = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
             .filterBounds(aoi)
             .filterDate(start_date, end_date)
            )

    # Get the list of dates in the collection
    dates = imcol.aggregate_array('system:time_start').getInfo()
    np_dates = [np.datetime64(ee.Date(date).format('YYYY-MM-dd').getInfo()) for date in dates]
    unique_dates = np.unique(np_dates)
    print(f'Dynamic World Data Dates: {unique_dates}')

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
        dw_label_composite = dw_label_composite.toFloat()

        # For other bands, calculate mean probability across all pixels.
        dw_bands_composite = date_col.select(['water', 'trees', 'grass', 'flooded_vegetation', 
        'crops', 'shrub_and_scrub', 'built', 'bare', 'snow_and_ice']).reduce(ee.Reducer.mean()).toFloat()

        # Combine the label band to the other class bands.
        dw_composite = dw_bands_composite.addBands(dw_label_composite)

        # Check the percentage of nodata pixels.
        pct_nodata = check_pct_null(dw_composite, aoi)
        print(f"Percentage of nodata pixels: {pct_nodata}%")

        # If the image is entirely nodata (i.e. the bit of the scene )
        if pct_nodata == 100:
            print(f"Composite for {date_str} contains only nodata. Skipping...")
            continue

        # Construct output file path.
        out_path = os.path.join(out_dir, f'composite_{date_str}.tif')
        
        # Export the dynamic world data as a GeoTIFF.
        geemap.ee_export_image(dw_composite, filename = out_path, scale = 10, region=aoi)

    return 



def fetch_dynamic_world_raw(aoi_path, start_date, end_date, out_dir):
    """Export the raw data without compositing, including full nodata images."""
    # Initialize Earth Engine
    ee.Initialize()

    # Set start and end dates as ee.Date
    start_date = ee.Date(start_date)
    end_date = ee.Date(end_date)
    
    # Get AOI bounding box polygon
    #aoi = get_bbox(aoi_path)
    aoi = get_boundaries(aoi_path)
    
    # Load the Dynamic World image collection for the AOI and dates of interest
    imcol = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
             .filterBounds(aoi)
             .filterDate(start_date, end_date)
            )

    # Get the number of images in the collection
    num_images = imcol.size().getInfo()
    print(f'Number of images in the collection: {num_images}')

    # Get the list of all images in the collection
    image_list = imcol.toList(num_images)

    for i in range(num_images):
        # Get the image by index
        image = ee.Image(image_list.get(i))
        
        # Extract the image's acquisition time
        image_date = ee.Date(image.get('system:time_start')).format('YYYY-MM-dd').getInfo()
        print(f'Processing image for date: {image_date}')

        band_names = image.bandNames().getInfo()
        print(f'Band names for image on {image_date}: {band_names}')

        # Extract the image's system:index
        system_index = image.get('system:index').getInfo()

        # Construct output file path
        out_path = os.path.join(out_dir, f'raw_{system_index}.tif')

        # Export the raw image as a GeoTIFF.
        geemap.ee_export_image(image, filename=out_path, scale=10, region=aoi)
        print(f"Exported raw image to {out_path}")
    
    return


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
    dynamic_world_tif = fetch_dynamic_world(aoi_path, start_date, end_date, out_dir)
    dynamic_world_tif = fetch_dynamic_world_raw(aoi_path, start_date, end_date, out_dir)


