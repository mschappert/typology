import arcpy
import os

# Area
metric = "area"
# # 1990-1995
# earlier = r"D:\typology\data\mw_area\1990_area_1km.tif"
# later = r"D:\typology\data\mw_area\1995_area_1km.tif"
# output  = r"D:\typology\data\img_diff\area"

# def raster_difference_and_save(earlier, later, output, metric):
#     # Naming convention from your example
#     base1 = os.path.splitext(os.path.basename(earlier))[0]
#     base2 = os.path.splitext(os.path.basename(later))[0]
#     year1 = ''.join(filter(str.isdigit, base1))[:4]
#     year2 = ''.join(filter(str.isdigit, base2))[:4]
#     label1 = year1[-2:]
#     label2 = year2[-2:]
#     out_name = f"{label1}-{label2}_{metric}.tif"
#     out_path = os.path.join(output, out_name)

#     print("Calculating raster difference...")
#     # Raster math
#     raster_earlier = arcpy.Raster(earlier)
#     raster_later = arcpy.Raster(later)
#     diff_raster = raster_later - raster_earlier
#     print("Saving raster")
#     diff_raster.save(out_path)
#     print(f"Saved: {out_path}")
#     return out_path

# raster_difference_and_save(earlier=earlier, later=later, output=output, metric=metric)

# def raster_difference(earlier, later, output, metric):
#     base1 = os.path.splitext(os.path.basename(earlier))[0]
#     base2 = os.path.splitext(os.path.basename(later))[0]
#     year1 = ''.join(filter(str.isdigit, base1))[:4]
#     year2 = ''.join(filter(str.isdigit, base2))[:4]
#     label1 = year1[-2:]
#     label2 = year2[-2:]
#     out_name = f"{label1}-{label2}_{metric}.tif"
#     out_path = os.path.join(output, out_name)

#     # Load rasters
#     raster_earlier = arcpy.Raster(earlier)
#     raster_later = arcpy.Raster(later)

#     print("Calculating raster difference...")
#     # Subtract rasters
#     diff_raster = raster_later - raster_earlier

#     # Build mask for NoData: Where either input has NoData, set output to NoData
#     mask = (raster_earlier == 0) | (raster_later == 0)
#     masked_diff = arcpy.sa.SetNull(mask, diff_raster)

#     print("Saving raster")
#     # Save the result
#     masked_diff.save(out_path)
#     print(f"Saved: {out_path}")
#     return out_path

# raster_difference(earlier=earlier, later=later, output=output, metric=metric)

import arcpy
import os

years = [1990, 1995, 2000, 2005, 2010, 2015, 2020]

# metric = "area" 
# base_dir = "D:/typology/data/mw_area"
# output_dir = "D:/typology/data/img_diff/area"

# metric = "edge" 
# base_dir = "D:/typology/data/mw_edge"
# output_dir = "D:/typology/data/img_diff/edge"

metric = "pn" 
base_dir = "D:/typology/data/mw_pn"
output_dir = "D:/typology/data/img_diff/pn"


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
    mask = (raster_earlier == 0) | (raster_later == 0)
    final_raster = arcpy.sa.SetNull(mask, diff_raster)
    final_raster.save(out_path)
    print(f"Saved: {out_path}")
    return out_path

# Loop through each pair of years
for i in range(len(years) - 1):
    earlier = f"{base_dir}/{years[i]}_{metric}_1km.tif"
    later   = f"{base_dir}/{years[i+1]}_{metric}_1km.tif"
    raster_difference_zero_bg_to_nodata(earlier, later, output_dir, metric)
