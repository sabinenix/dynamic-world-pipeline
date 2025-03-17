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


def check_pct_null(image, aoi, crs, crs_transform):
    # Get the internal mask of the image and invert so nodata is 1.
    nodata_mask = image.mask().Not()

    # Get the number of non-null pixels with .count().
    total_pixels = image.reduceRegion(
        reducer=ee.Reducer.count(), 
        geometry=aoi, 
        maxPixels=1e8).get('label')


    # Get the number of nodata pixels from the mask.
    nodata_pixels = nodata_mask.reduceRegion(
        reducer=ee.Reducer.sum(), 
        geometry=aoi, 
        maxPixels=1e8).get('label')

    # Compute percentage of nodata pixels
    pct_nodata = nodata_pixels.multiply(100).divide(total_pixels.add(nodata_pixels))

    return pct_nodata

# TODO: Switch to using lookup table approach here
def get_utm_projection(geojson_path, geometry):
    """
    Get the UTM projection for the AOI using geopandas, using transform from Dynamic World.
    """
    
    # Get the UTM zone CRS from geopandas.
    gdf = gpd.read_file(geojson_path)
    utm_crs = gdf.estimate_utm_crs()
    epsg = utm_crs.to_epsg()
    target_crs = f'EPSG:{epsg}'

    # Get the Dynamic World collection that intersects the AOI.
    collection = ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1').filterBounds(geometry)
    
    # Add a property to each image indicating if it matches our target CRS
    def add_crs_match(img):
        proj = img.projection()
        crs = ee.String(proj.crs())
        match = crs.equals(target_crs)
        return img.set('crs_match', match)
    
    images_with_matches = collection.map(add_crs_match)
    
    # Get the first image that matches the target CRS.
    matching_image = images_with_matches.filter(ee.Filter.eq('crs_match', 1)).first()
    
    # In case no images match (shouldn't be the case), use the first image.
    first_image = collection.first()
    
    # Check if any images match the target CRS.
    matching_size = images_with_matches.filter(ee.Filter.eq('crs_match', 1)).size().getInfo()
    
    if matching_size > 0:
        final_image = matching_image
    else:
        final_image = first_image
    
    # Get projection info.
    proj_info = final_image.projection().getInfo()
    
    return {
        'crs': target_crs,  # Always use the target CRS (UTM zone from AOI)
        'transform': proj_info['transform']
    }

def n_valid_pixels(image, aoi):
    """
    Get number of valid pixels in an image for a given AOI
    """
    # Get the number of non-null pixels with .count().
    total_pixels = image.reduceRegion(
        reducer=ee.Reducer.count(), 
        geometry=aoi, 
        maxPixels=1e8).get('label')

    return total_pixels

def fetch_dynamic_world(aoi, start_date, end_date, out_dir, utm_proj):
    """
    Function to acquire Dynamic World rasters for a specific polygon and date range.
    """
    # Set both the start and end date to the date given (returning daily composites).
    start_date = ee.Date(start_date)
    end_date = ee.Date(end_date)
    
    # Load the Dynamic World image collection for the aoi and dates of interest.
    imcol = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
             .filterBounds(aoi)
             .filterDate(start_date, end_date)
             # Clip to only the AOI.
             .map(lambda image: image.clip(aoi))
             # Calculate number of valid pixels.
             .map(lambda image: image.set('n_valid_pixels', n_valid_pixels(image, aoi)))
             # Filter out images with no valid pixels.
             .filter(ee.Filter.gt('n_valid_pixels', 0)))

    # Get list of dates in the collection (more efficient method than previous).
    unique_dates = imcol.aggregate_array('system:time_start') \
        .map(lambda date: ee.Date(date).format('YYYY-MM-dd')) \
        .distinct() \
        .getInfo()

    # Initialize list to store export tasks.
    tasks = []  

    for date in unique_dates:
    
        # Set start date (inclusive) and end date (exclusive) to get only images from one day.
        date_str = str(date)
        ee_start_date = ee.Date(date_str)
        ee_end_date = ee_start_date.advance(1, 'day')

        print(f'Computing composite for date: {date_str}')

        # Filter the image collection to the start and end dates.
        date_col = imcol.filterDate(ee_start_date, ee_end_date)

        # For label band, reduce to the most probable land cover type (using mode reducer).
        dw_label_composite = date_col.select('label').mode().toFloat()

        # For other bands, calculate mean probability across all pixels.        
        lc_bands = ['water', 'trees', 'grass', 'flooded_vegetation', 
                    'crops', 'shrub_and_scrub', 'built', 'bare', 'snow_and_ice']
        dw_bands_composite = date_col.select(lc_bands).mean().toFloat()

        # Combine the label band to the other class bands.
        dw_composite = dw_bands_composite.addBands(dw_label_composite)

        # Construct output file path.
        file_name = f'composite_{date_str}'

        # Set the NoData Value to -9999
        # https://developers.google.com/earth-engine/guides/exporting_images#nodata
        noDataVal = -9999
        dw_composite.unmask(noDataVal, sameFootprint = True)

        # Export the dynamic world data as a GeoTIFF.
        task = ee.batch.Export.image.toDrive(dw_composite, 
                                            description = file_name,
                                            folder = out_dir,
                                            region = aoi,
                                            crs = utm_proj['crs'],
                                            crsTransform = utm_proj['transform'],
                                            maxPixels = 1E7, # Setting max to 1 million pixels (~1000km^2 with 10m pixels) as safeguard
                                            fileFormat = 'GeoTIFF',
                                            formatOptions = {'cloudOptimized': True,
                                                             'noData': noDataVal})
        
        tasks.append(task)
        task.start()
        print(f"Export task started for {date_str}")

    return tasks

if __name__ == "__main__":
    # Create a context using the certifi bundle.
    context = ssl.create_default_context(cafile=certifi.where())

    # Initialize earth engine and geemap.
    #ee.Authenticate() # Only need to authenticate once. 
    ee.Initialize(opt_url="https://earthengine.googleapis.com", project="dynamic-world-pipeline")
    geemap.ee_initialize()

    # Read config information from yml file.
    with open("config.yml", "r") as yml:
        config = yaml.safe_load(yml)

    start_date = config['start-date']
    end_date = config['end-date']
    out_dir = config['out-dir']
    aoi_path = config['aoi-path']

    # Get aoi as a EE polygon object.
    aoi = get_boundaries(aoi_path)

    # Get UTM projection and transform from center of AOI.
    utm_proj = get_utm_projection(aoi_path, aoi)

    # Call function to export dynamic world raster.
    tasks = fetch_dynamic_world(aoi, start_date, end_date, out_dir, utm_proj)

    # for task in tasks:
    #     print(f"task: {task}")
    #     print(f"Task ID: {task.id}")
    #     status = task.status()
    #     print(f"Task {status['description']} is {status['state']}")

    #print(ee.data.getTaskStatus("TUWAPFM6RG6IDXAPD7E3CKMK"))


