# this creates the binary mask before calculating the z-scores

import arcpy
import arcpy.sa
import os
import tempfile

arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True


# Fix your paths here (remove asterisks and escape properly)
folder_path = r"D:\typology\data\mw_pn"
output_folder = r"D:\typology\data\TS_mask"
metric = "pn"  # or "edge", "pn"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

raster_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith('.tif')]

# Process rasters: convert NoData to 0 and store in memory
temp_dir = tempfile.gettempdir()

rasters_rc = []
for i, r in enumerate(raster_list):
    raster = arcpy.sa.Raster(r)
    raster_rc = arcpy.sa.Con(arcpy.sa.IsNull(raster), 0, raster)
    temp_name = os.path.join(temp_dir, f"raster_rc_{i}.tif")
    raster_rc.save(temp_name)  # save to a temp file instead of in_memory
    rasters_rc.append(arcpy.sa.Raster(temp_name))

# Sum rasters incrementally to keep memory usage lower
sum_raster = rasters_rc[0]
for raster in rasters_rc[1:]:
    sum_raster = arcpy.sa.Plus(sum_raster, raster)

# Create binary mask: 0 where sum == 0, else 1
binary_mask = arcpy.sa.Con(sum_raster == 0, 0, 1)

output_path = os.path.join(output_folder, f"{metric}_mask.tif")
binary_mask.save(output_path)

print(f"Binary mask saved at: {output_path}")

#arcpy.CheckInExtension("Spatial")