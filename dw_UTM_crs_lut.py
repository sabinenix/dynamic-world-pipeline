import ee
import pandas as pd
import concurrent.futures

ee.Initialize()

# Get DW image collection for a random year
DW = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
    .filter(ee.Filter.date('2019-01-01', '2019-12-31'))
    .map(lambda image: image.set('crs', image.projection().crs()))
    )

print(DW.first().get('crs').getInfo())

# Create list of all UTM epsg codes; 
# UTM codes range from 1 to 60, starting with 326 in the northern hemisphere and 327 in the southern hemisphere
utm_epsg_codes = [f"EPSG:326{zone:02d}" for zone in range(1, 61)] + [f"EPSG:327{zone:02d}" for zone in range(1, 61)]

# Function to get affine transform for a given UTM epsg code
def get_affine_transform(utm_epsg_code):
    print(f"Getting affine transform for {utm_epsg_code}")
    img = DW.filter(ee.Filter.eq('crs', utm_epsg_code)).first().getInfo()

    transform = img['bands'][0].get('crs_transform', None)

    # Manually correct second scale parameter in transform; should be negative [10, 0, 390450, 0, 10, 6090450]
    transform = transform[:4] + [-abs(transform[4])] + transform[5:]

    UTM_zone = img['properties'].get('system:index', None).split('_')[-1]

    return {
        'crs': utm_epsg_code,
        'transform': transform,
        'UTM_zone': UTM_zone
    }

# Fetch affine transforms in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=18) as executor:
    affine_transforms = dict(zip(utm_epsg_codes, executor.map(get_affine_transform, utm_epsg_codes)))

# Create pandas dataframe from dict 
lut = pd.DataFrame(affine_transforms).T

# Save dataframe to csv
lut.to_csv('dw_UTM_crs_lut.csv', index=False)

# test for first row
utm_epsg_code = utm_epsg_codes[1]