# Science Pipeline for Dynamic World
### Updated: 4 December 2024

### Google Earth Engine Access


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
