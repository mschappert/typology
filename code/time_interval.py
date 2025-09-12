import arcpy
import os

years = [1990, 1995, 2000, 2005, 2010, 2015, 2020]

metric = "edge" 
base_dir = "D:/typology/data/3_MovingWindow/mw_edge"#"B:/Mikayla/DATA/Projects/AF/Typology_collection9/3_MovingWindow/mw_edge" #"D:/typology/data/mw_area"
output_dir = "D:/typology/data/4_TS_and_TI/img_diff"#"B:/Mikayla/DATA/Projects/AF/Typology_collection9/4_TS_and_TI/img_diff" #"D:/typology/data/img_diff/area"

# metric = "edge" 
# base_dir = "D:/typology/data/mw_edge"
# output_dir = "D:/typology/data/img_diff/edge"

# metric = "pn" 
# base_dir = "D:/typology/data/mw_pn"
# output_dir = "D:/typology/data/img_diff/pn"


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
    mask = (raster_earlier == 0) & (raster_later == 0)  # this makes it so if both are 0, set to NoData
    final_raster = arcpy.sa.SetNull(mask, diff_raster)
    final_raster.save(out_path)
    print(f"Saved: {out_path}")
    return out_path

# Loop through each pair of years
for i in range(len(years) - 1):
    earlier = f"{base_dir}/{years[i]}_{metric}_1km.tif"
    later   = f"{base_dir}/{years[i+1]}_{metric}_1km.tif"
    raster_difference_zero_bg_to_nodata(earlier, later, output_dir, metric)