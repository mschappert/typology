import arcpy
from arcpy.sa import *

# Check out the Spatial Analyst extension
arcpy.CheckOutExtension("Spatial")

# Set environment workspace if needed
arcpy.env.workspace = r"C:\path\to\your\workspace"

# Input raster
input_raster = Raster("input_raster.tif")

# Calculate mean and standard deviation of input raster
mean_result = arcpy.GetRasterProperties_management(input_raster, "MEAN")
stddev_result = arcpy.GetRasterProperties_management(input_raster, "STD")

mean_val = float(mean_result.getOutput(0))
stddev_val = float(stddev_result.getOutput(0))

# Perform z-score standardization: (Raster - Mean) / StdDev
standardized_raster = (input_raster - mean_val) / stddev_val

# Save the output raster
standardized_raster.save("standardized_raster.tif")

# Check in the Spatial Analyst extension
arcpy.CheckInExtension("Spatial")
