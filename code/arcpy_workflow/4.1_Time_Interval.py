import arcpy
import os
########################################## Parammeters 
main_workspace = r"D:\Mikayla\site_selection" # main workspace for the project

# Example: years = [1990, 1995, 2000, 2005, 2010, 2015, 2020] or [1990, 2020]
years = [2020, 2024]

metric = "pn" # "edge" or "pn" or "area"
# base_dir = the moving window output folder for the metric, e.g. 3_MovingWindow/mw_area, mw_edge, or mw_pn
base_dir = r"D:\Mikayla\site_selection\3_MovingWindow\mw_pn" # area, edge, or pn,  must match metric

########################################## Code

# Create 4_TS_and_TI/img_diff/metric folder (sits beside 3_MovingWindow)
output_dir = os.path.join(main_workspace, "4_TS_and_TI", "img_diff", metric)
os.makedirs(output_dir, exist_ok=True)

def raster_difference_zero_bg_to_nodata(earlier, later, output, metric):
    base1 = os.path.splitext(os.path.basename(earlier))[0]
    base2 = os.path.splitext(os.path.basename(later))[0]
    year1 = ''.join(filter(str.isdigit, base1))[:4]
    year2 = ''.join(filter(str.isdigit, base2))[:4]
    label1 = year1[-2:]
    label2 = year2[-2:]
    out_name = f"{label1}-{label2}_{metric}.tif"
    out_path = os.path.join(output, out_name)

    raster_earlier = arcpy.Raster(earlier)
    raster_later = arcpy.Raster(later)
    diff_raster = raster_later - raster_earlier
    mask = (raster_earlier == 0) & (raster_later == 0) # this makes it so if both are 0, set to NoData
    final_raster = arcpy.sa.SetNull(mask, diff_raster)
    final_raster.save(out_path)
    print(f"Saved: {out_path}")
    return out_path

# Loop through each pair of years
for i in range(len(years) - 1):
    earlier = os.path.join(base_dir, f"{years[i]}_{metric}_1km.tif")
    later = os.path.join(base_dir, f"{years[i+1]}_{metric}_1km.tif")
    raster_difference_zero_bg_to_nodata(earlier, later, output_dir, metric)