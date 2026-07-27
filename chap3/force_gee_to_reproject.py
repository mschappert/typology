import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling

ref_path = r"D:\site_selection\5_Typology\combined_rc_interval\20-24_combined_rc.tif"
src_path = r"D:\site_selection\MB_SecVeg_Deforestation_2023\MB_c10_secFveg_2023_P.tif" #r"D:\site_selection\GEDI_mean_height_2024"
out_path = r"D:\site_selection\MB_SecVeg_Deforestation_2023\MB_c10_secFveg_2023_Pv2.tif"

with rasterio.open(ref_path) as ref:
    ref_profile = ref.profile.copy()
    ref_transform = ref.transform
    ref_crs = ref.crs
    ref_shape = (ref.height, ref.width)

with rasterio.open(src_path) as src:
    dst_data = np.empty(ref_shape, dtype=src.dtypes[0])
    reproject(
        source=src.read(1),
        destination=dst_data,
        src_transform=src.transform,
        src_crs=src.crs,
        dst_transform=ref_transform,
        dst_crs=ref_crs,
        dst_nodata= 0, ########### check to see if this is correct
        resampling=Resampling.nearest
    )

ref_profile.update(dtype=dst_data.dtype)
with rasterio.open(out_path, "w", **ref_profile) as dst:
    dst.write(dst_data, 1)

print("Done! Output:", out_path)