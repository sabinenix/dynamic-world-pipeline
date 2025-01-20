# Science Testing for Dynamic World
### Updated: 20 January 2025

### Files

- `dynamic-world-exports-daily.py` -> Script that computes daily composites for dynamic world tifs over a specified area of interest and time range.
- `dynamic-world-exports.py` -> The science usage testing for downloading dynamic world tifs for an area of interest and specified time range.
- `Dynamic World LULC Pipeline.ipynb` -> (old) A Jupyter Notebook including some more set up code and plotting outputs.

### Notes & Considerations 

- `dynamic-world-exports-daily.py` has two main functions - one that downloads daily composites for each day when there is valid data over the AOI, and one that downloads raw images without compositing. Note that sometimes there are more dates represented in the raw images than the composite images because the raw images are marked as overlapping the AOI even if the part of the image that covers the AOI is entirely nodata.
- When exporting the images locally, the bands are named 'Band 01', 'Band 02', 'Band 03', etc. It appears that the bands keep their original names when exporting to cloud storage, but I have not been able to verify this. If this is not the case, I can manually rename the bands so they maintain their original names. 
- The daily composites use `ee.Reducer.mean()` for the following bands: 'water', 'trees', 'grass', 'flooded_vegetation', 'crops', 'shrub_and_scrub', 'built', 'bare', 'snow_and_ice', and `ee.Reducer.mode()` for the 'label' band. Note that in the case of a tie, `ee.Reducer.mode()` appears to select the lower number, which may not be the most scientifically valid way of computing modes in our use case (e.g., if two images say the highest probability class for a given pixel is Band 5, and the other two images say the highest probability class is 7, it appears that `ee.Reducer.mode()` will select 5 for no reason other than that it is lower than 7.) This is most likely not significant over large areas / large numbers of pixels.
- Note that the 'label' band contains the index of the highest probability band, and all other bands contain the percentage probability of the pixel being that class.

### Google Earth Engine Access
This pipeline requires a Google Earth Engine account and an existing project to have been set up. See the Jupyter Notebook for more details on this set up process.

### Config File Structure

- The pipeline is set up to run using a config file (`config.yml`)
- Specify the path to the area of interest geojson (`aoi-path`) and the directory in which to store outputs (`out-dir`)
- Provide a target date (`target-date`) around which to composite the dynamic world data
- Provide an initial date buffer (`date-buffer`) to composite data over, which will be increased incrementally until the amount of nodata in the area of interest drops below the specified threshold
- Provide a threshold for the max amount of nodata that is acceptable (`nodata-threhsold`) (e.g. a threshold of 20% means that the temporal range will be expanded until less than 20% of the area of interest is nodata)
- An example structure for this file is as follows:
    ```
    aoi-path: "aoi.geojson"
    out-dir: "outputs"
    target-date: '2021-06-07'
    date-buffer: 1
    nodata-threshold: 20
    ```
