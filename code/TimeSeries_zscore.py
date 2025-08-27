import arcpy
import os
import multiprocessing
from functools import partial

#########################################  
# ArcPy configuration
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("Spatial")
cores = multiprocessing.cpu_count()

# Environment settings
arcpy.env.pyramid = "NONE"

#########################################
# Parameters
input_dir = r"D:\typology\data\trend\area_rg_MK_tau.tif"
output_dir = r"D:\typology\data\TS_zscore"
mask = r"D:\typology\data\TS_mask\area_mask.tif"
metric = "area" # edge, area, or pn 

#########################################

def init_worker():
    import arcpy
    arcpy.env.overwriteOutput = True
    arcpy.env.parallelProcessingFactor = "0"
    arcpy.CheckOutExtension("Spatial")
    
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

def zscore_standardization(input_path, output_dir, mask_raster=None, metric=metric):
    try:
        # basename = os.path.basename(input_path)
        # name = os.path.splitext(basename)[0]
        output_path = os.path.join(output_dir, f"{metric}_MK_tau_z.tif")
        
        if arcpy.Exists(output_path):
            print(f"Output already exists: {output_path}")
            return output_path
            
        input_ras = arcpy.sa.Raster(input_path)
        
        if mask_raster:
            mask_ras = arcpy.sa.Raster(mask_raster)
            masked_input = arcpy.sa.Con(mask_ras == 1, input_ras)
        else:
            masked_input = input_ras
        
        mean_result = arcpy.GetRasterProperties_management(masked_input, "MEAN")
        stddev_result = arcpy.GetRasterProperties_management(masked_input, "STD")
        
        mean_val = float(mean_result.getOutput(0))
        stddev_val = float(stddev_result.getOutput(0))
        
        if mask_raster:
            standardized_raster = arcpy.sa.Con(mask_ras == 1, (input_ras - mean_val) / stddev_val)
        else:
            standardized_raster = (input_ras - mean_val) / stddev_val
        
        standardized_raster.save(output_path)
        print(f"Z-score standardization saved: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Z-score error for {input_path}: {str(e)}")
        return None

if __name__ == "__main__":  
    process_rasters(
        zscore_standardization, 
        input_dir, 
        use_multiprocessing=False,
        output_dir=output_dir,
        mask_raster=mask,
        metric=metric
    )