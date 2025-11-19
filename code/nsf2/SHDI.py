import os
import arcpy
import multiprocessing
import re
import time
import sys
import numpy as np
from functools import partial
##########
# ArcPy Configurations
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
arcpy.CheckOutExtension("Spatial")
cores = multiprocessing.cpu_count()
##########
# Parameters
shdi_in = r"E:/NSF2_Naoya_Mikayla/Data/MB_v9_AtlanticForest_Landcover/orig_P"
shdi_out = r"E:/NSF2_Naoya_Mikayla/Data/SHDI_landcover"
shdi_radius = 1000  # meters

#########################################    

def init_worker():
    """Initialize ArcPy environment for multiprocessing workers"""
    import arcpy
    arcpy.env.overwriteOutput = True
    arcpy.env.parallelProcessingFactor = "0"
    arcpy.CheckOutExtension("Spatial")
    arcpy.env.pyramid = "NONE"
    arcpy.env.rasterStatistics = "NONE"

def get_year(filename):
    match = re.search(r"(\d{4})", filename)
    return match.group(1) if match else ""

def process_rasters(process_func, input_dir, use_multiprocessing=False, **kwargs):
    arcpy.env.workspace = input_dir
    rasters = arcpy.ListRasters()
    
    if not rasters:
        print(f"No rasters found in directory: {input_dir}")
        return []
    
    print(f"Processing {len(rasters)} rasters...")

    if use_multiprocessing:
        arcpy.env.parallelProcessingFactor = "0"
        input_paths = [os.path.join(input_dir, r) for r in rasters]
        func = partial(process_func, **kwargs)
        
        print(f"Using {cores} processes for multiprocessing")
        with multiprocessing.Pool(processes=cores, initializer=init_worker) as pool:
            outputs = pool.map(func, input_paths)
    else:
        arcpy.env.parallelProcessingFactor = "80%"
        outputs = []
        for raster in rasters:
            input_path = os.path.join(input_dir, raster)
            result = process_func(input_path, **kwargs)
            outputs.append(result)
    
    success_count = sum(1 for p in outputs if p)
    print(f"Process complete: {success_count}/{len(rasters)} succeeded")
    return outputs

#######################################

def shdi_window(input_raster, output_dir=shdi_out, radius=shdi_radius):
    """Calculate Shannon Diversity Index using moving window"""
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_shdi.tif")
        
        if not arcpy.Exists(output_path):
            print(f"Processing SHDI for {basename}")
            
            desc = arcpy.Describe(input_raster) # get raster properties
            arr = arcpy.RasterToNumPyArray(input_raster) # load raster as numpy array
            
            # Convert meters to pixels (same as moving window would use)
            cell_size = desc.meanCellWidth
            pixel_radius = int(radius / cell_size)
            
            # set nodata to 0
            nodata_val = 0
            nodata_mask = arr == nodata_val
            output_arr = np.full_like(arr, nodata_val, dtype=np.float32)
            rows, cols = arr.shape
            
            for i in range(rows):
                for j in range(cols):
                    if nodata_mask is not None and nodata_mask[i, j]:
                        continue
                    # define window boundaries    
                    r0, r1 = max(i - pixel_radius, 0), min(i + pixel_radius + 1, rows)
                    c0, c1 = max(j - pixel_radius, 0), min(j + pixel_radius + 1, cols)
                    window = arr[r0:r1, c0:c1]
                    
                    # mask nodata values
                    if nodata_mask is not None:
                        window_mask = nodata_mask[r0:r1, c0:c1]
                        window = window[~window_mask]
                    
                    if len(window) == 0:
                        continue
                    
                    # SHDI calculation    
                    vals, counts = np.unique(window, return_counts=True) # count each landcover type
                    ps = counts / counts.sum() # calculatqe proportions
                    ps_log = np.where(ps > 0, np.log(ps), 0) # log of proportion (if 0 = 0 not 0 = log(0))
                    shdi = -np.sum(ps * ps_log) # SHDI formula
                    output_arr[i, j] = shdi
            
            # convert back to raster
            out_raster = arcpy.NumPyArrayToRaster(
                output_arr,
                arcpy.Point(desc.extent.XMin, desc.extent.YMin),
                desc.meanCellWidth, desc.meanCellHeight
            )
            # preserve coordinate system
            arcpy.DefineProjection_management(out_raster, desc.spatialReference)
            out_raster.save(output_path)
            print(f"SHDI successful: {output_path}")
            
        return output_path
        
    except Exception as e:
        print(f"SHDI error: {str(e)}")
        return None

# ==========
if __name__ == "__main__":
    print("Starting Processing")
    
    ## Run SHDI stage
    print("Starting SHDI")
    shdi_start = time.time()
    shdi_results = process_rasters(
        shdi_window,
        shdi_in,
        use_multiprocessing=True,
        output_dir=shdi_out,
        radius=shdi_radius
    )
    shdi_duration = time.time() - shdi_start
    print(f"SHDI completed in {shdi_duration:.2f} seconds")
    
    print("Processing complete!")
