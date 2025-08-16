import os
import xarray as xr
import rioxarray as rxr

def img_diff(image1, image2, output_dir, metric):
    """Calculate difference between two raster images and save result."""
    os.makedirs(output_dir, exist_ok=True)
    
    img1 = rxr.open_rasterio(image1, chunks={'x': 1000, 'y': 1000})
    img2 = rxr.open_rasterio(image2, chunks={'x': 1000, 'y': 1000})
    
    diff = img2 - img1
    
    year1 = os.path.basename(image1).split('_')[0]
    year2 = os.path.basename(image2).split('_')[0]
    output_file = os.path.join(output_dir, f"{year1}_{year2}_{metric}_diff.tif")
    
    diff.rio.to_raster(output_file)
    return output_file