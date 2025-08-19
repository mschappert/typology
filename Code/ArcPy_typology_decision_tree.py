### Import Packages ###
import os
import arcpy
import arcpy.sa
import multiprocessing
import re
import sys
import time

### Parameters ###
# Remap - Time Series
# run each metric type separately to remap
# input_raster = r"W:\Mikayla\DATA\Projects\AF\Typology_collection9\ETM\31year\area\area_rg_MK_Z.tif"
# output_dir = r"C:\Users\mksch\Desktop" #"E:\GWB_Working\remap"
# metric_type = "area" # or "area", "pn"

# Remap - Interval
input_raster = r"D:\typology\data\img_diff\edge\*.tif" # change folder for each metric type
output_dir = r"D:\typology\data\reamp" #"E:\GWB_Working\remap"
metric_type = "edge" # "edge", "area", or "pn"

# Combine Rasters
# all rasters should be in the same folder- it searches by file name to combine by year
combine_input = r"D:\typology\data\remap_interval"#r"D:\typology\data\old_remap_new_combine"# r"D:\typology\data\remap_test"
combine_output = r"D:\typology\data\combined_interval" #r"D:\typology\data\old_remap_new_combine"

# Reclassify and Add Typology Names
rc_input = r"D:\typology\data\combined_interval"#r"E:\GWB_Working\combined_output"
rc_output = r"D:\typology\data\rc_combined_interval"#r"E:\GWB_Working\typology_output"

### Functions ###
def get_year(filename):
    # Extract year range pattern like "90-95" from filename (eg. 90-95_area) using regex
    match = re.search(r"(\d{2}-\d{2})", filename)
    return match.group(1) if match else ""
    
def remap_time_series(input_dir, output_dir, metric):
    """
    Remap raster values based on the specified metric (patch, area, edge).
    """
    try:
        print(f"Setting workspace to: {input_dir}")
        arcpy.env.workspace = input_dir
        rasters = arcpy.ListRasters()
        print(f"Found {len(rasters)} rasters: {rasters}")
        
        if not rasters:
            print("No rasters found in workspace!")
            return False
            
        for raster in rasters:
            input_raster_path = os.path.join(input_dir, raster)
            basename = os.path.basename(input_raster_path)
            year = get_year(basename)
            
            # Define remap rules based on the metric
            if metric == "pn":
                output_path = os.path.join(output_dir, f"{year}_pn_rmp.tif")
                # remap_rules = [
                #     (-70, -1.01, 100),
                #     (-1, -0.11, 200),
                #     (-0.1, 0.1, "NODATA"),
                #     (0.11, 1, 200),
                #     (1.01, 70, 300)
                # ]
                remap_rules = [
                    (-70, -1.01, 100),
                    (-1, 1, 200),
                    (1.01, 70, 300)
                ]
                remap = arcpy.sa.RemapRange(remap_rules)
                output_raster = arcpy.sa.Reclassify(input_raster_path, "Value", remap, "NODATA")
            elif metric == "area":
                output_path = os.path.join(output_dir, f"{year}_area_rmp.tif")
                remap_rules = [
                    (-70, -1.01, 10),
                    (-1, -0.01, 20),
                    (0.01, 1, 20),
                    (1.01, 70, 30)
                ]
                remap = arcpy.sa.RemapRange(remap_rules)
                output_raster = arcpy.sa.Reclassify(input_raster_path, "Value", remap, "NODATA")
            elif metric == "edge":
                output_path = os.path.join(output_dir, f"{year}_edge_rmp.tif")
                remap_rules = [
                    (-70, -1.01, 1),
                    (-1, 1, 2),
                    (1.01, 70, 3)
                ]
                remap = arcpy.sa.RemapRange(remap_rules)
                output_raster = arcpy.sa.Reclassify(input_raster_path, "Value", remap, "NODATA")
            else:
                raise ValueError("Invalid metric specified: {}".format(metric))

            if not arcpy.Exists(output_path):
                output_raster.save(output_path)
                print(f"Remapped successful: {output_path}")
        return True
    except Exception as e:
        print(f"Remap error: {str(e)}")
        return None
    
def remap_time_interval(input_dir, output_dir, metric):
    """
    Remap raster values based on the specified metric (patch, area, edge).
    """
    try:
        #print(f"Setting workspace to: {input_dir}")
        #arcpy.env.workspace = input_dir
        #rasters = arcpy.ListRasters()
        #print(f"Found {len(rasters)} rasters: {rasters}")
        rasters = [f for f in os.listdir(input_dir) if f.endswith('.tif')]
        print(f"Found {len(rasters)} rasters: {rasters}")
        
        if not rasters:
            print("No rasters found")
            return False
            
        for raster in rasters:
            input_raster_path = os.path.join(input_dir, raster)
            basename = os.path.basename(input_raster_path)
            year = get_year(basename)
            
            # Define remap rules based on the metric
            if metric == "pn":
                output_path = os.path.join(output_dir, f"{year}_pn_rmp.tif")
                remap_rules = [
                    (-70, -0.000001, 100),
                    (0, 0, 200),
                    (0.000001, 70, 300)
                ]
                remap = arcpy.sa.RemapRange(remap_rules)
                output_raster = arcpy.sa.Reclassify(input_raster_path, "Value", remap, "NODATA")
            elif metric == "area":
                output_path = os.path.join(output_dir, f"{year}_area_rmp.tif")
                remap_rules = [
                    (-70, -0.000001, 10),
                    (0, 0, 20),
                    (0.000001, 70, 30)
                ]
                remap = arcpy.sa.RemapRange(remap_rules)
                output_raster = arcpy.sa.Reclassify(input_raster_path, "Value", remap, "NODATA")
            elif metric == "edge":
                output_path = os.path.join(output_dir, f"{year}_edge_rmp.tif")
                remap_rules = [
                    (-70, -0.000001, 1),
                    (0, 0, 2),
                    (0.000001, 70, 3)
                ]
                remap = arcpy.sa.RemapRange(remap_rules)
                output_raster = arcpy.sa.Reclassify(input_raster_path, "Value", remap, "NODATA")
            else:
                raise ValueError("Invalid metric specified: {}".format(metric))

            if not arcpy.Exists(output_path):
                output_raster.save(output_path)
                print(f"Remapped successful: {output_path}")
        return True
    except Exception as e:
        print(f"Remap error: {str(e)}")
        return None

# This is the original combine which only calculates values when they over lap (100 + 20 + 2 = 122, NOT 100 + 20 + 0 = 0)
# def combine_by_year(input_dir, output_dir):
#     """Automatically combine edge, area, and patch rasters by year."""
#     edge_files = [f for f in os.listdir(input_dir) if f.endswith('_edge_rmp.tif')]
    
#     for edge_file in edge_files:
#         year = get_year(edge_file)
#         area_file = f"{year}_area_rmp.tif"
#         patch_file = f"{year}_pn_rmp.tif"
        
#         edge_path = os.path.join(input_dir, edge_file)
#         area_path = os.path.join(input_dir, area_file)
#         patch_path = os.path.join(input_dir, patch_file)
        
#         if os.path.exists(area_path) and os.path.exists(patch_path):
#             output_path = os.path.join(output_dir, f"{year}_combined.tif")
#             try:
#                 combined = arcpy.sa.Raster(edge_path) + arcpy.sa.Raster(area_path) + arcpy.sa.Raster(patch_path)
#                 combined.save(output_path)
#                 arcpy.management.BuildRasterAttributeTable(output_path)
#                 print(f"Combined raster created: {output_path}")
#             except Exception as e:
#                 print(f"Combine error: {str(e)}")

# takes all posibilties and adds them together (100 + 20 + 2 = 122, AND 100 + 20 + 0 = 0)      
def combine_by_year(input_dir, output_dir):
    """Automatically combine edge, area, and patch rasters by year."""
    edge_files = [f for f in os.listdir(input_dir) if f.endswith('_edge_rmp.tif')]
    
    for edge_file in edge_files:
        year = get_year(edge_file)
        area_file = f"{year}_area_rmp.tif"
        patch_file = f"{year}_pn_rmp.tif"
        
        edge_path = os.path.join(input_dir, edge_file)
        area_path = os.path.join(input_dir, area_file)
        patch_path = os.path.join(input_dir, patch_file)
        
        if os.path.exists(area_path) and os.path.exists(patch_path):
            output_path = os.path.join(output_dir, f"{year}_combined.tif")
            try:
                edge_r = arcpy.sa.Raster(edge_path)
                area_r = arcpy.sa.Raster(area_path)
                patch_r = arcpy.sa.Raster(patch_path)
                
                # Use Con to handle NODATA values
                # if no data exists, the addition off all 3 rasters will be no data so we apply no data to 0 to fix that
                combined = arcpy.sa.Con(arcpy.sa.IsNull(edge_r), 0, edge_r) + \
                          arcpy.sa.Con(arcpy.sa.IsNull(area_r), 0, area_r) + \
                          arcpy.sa.Con(arcpy.sa.IsNull(patch_r), 0, patch_r)
                
                combined.save(output_path)
                arcpy.management.BuildRasterAttributeTable(output_path)
                print(f"Combined raster created: {output_path}")
            except Exception as e:
                print(f"Combine error: {str(e)}")

# def combine_by_year(input_dir, output_dir):
#     try:
#         # Get edge files and find corresponding area/patch files
#         edge_files = [f for f in os.listdir(input_dir) if f.endswith('_edge_rmp.tif')]
        
#         for edge_file in edge_files:
#             year = get_year(edge_file)
#             area_file = f"{year}_area_rmp.tif"
#             pn_file = f"{year}_pn_rmp.tif"
            
#             # File paths
#             edge_path = os.path.join(input_dir, edge_file)
#             area_path = os.path.join(input_dir, area_file)
#             pn_path = os.path.join(input_dir, pn_file)
#             output_path = os.path.join(output_dir, f"{year}_combined.tif")
            
#             # Use Raster Calculator to combine
#             # expression = f'"{patch_path}" + "{area_path}" + "{edge_path}"'
#             # arcpy.gp.RasterCalculator_sa(expression, output_path)
#             print(f"Combined raster saved: {output_path}")
            
#         return True
#     except Exception as e:
#         print(f"Combine error: {str(e)}")
        
#         if os.path.exists(area_path) and os.path.exists(patch_path):
#             output_path = os.path.join(output_dir, f"{year}_combined.tif")
#             try:
#                 combined = arcpy.sa.Raster(edge_path) + arcpy.sa.Raster(area_path) + arcpy.sa.Raster(patch_path)
#                 combined.save(output_path)
#                 arcpy.management.BuildRasterAttributeTable(output_path)
#                 print(f"Combined raster created: {output_path}")
#             except Exception as e:
#                 print(f"Combine error: {str(e)}")

                
# Reclassify combined raster values to typology categories and add attribute table labels
def reclassify_typology(input_dir, output_dir):
    """Reclassify all combined rasters in the input directory."""
    combined_files = [f for f in os.listdir(input_dir) if f.endswith('_combined.tif')]
    
    for combined_file in combined_files:
        try:
            input_raster = os.path.join(input_dir, combined_file)
            basename = os.path.basename(input_raster)
            output_name = f"{os.path.splitext(basename)[0]}_rc.tif"
            output_path = os.path.join(output_dir, output_name)
            
            # Define typology recoding (original_value: new_val)
            recode_map = {
            #212: 0,  # background
            111: 1, 112: 1, 113: 1,  # attrition
            121: 2, 122: 2, 123: 2, 131: 2, 132: 2, 133: 2, 120: 2,  # aggregation (added 120 (decrease pn, increase area, no edge))
            211: 3,  # shrinkage
            213: 4,  # perforation
            221: 5, 223: 5,  # deformation
            222: 6, 220: 6, # persistent  # originally shift was jus 222
            231: 7, 232: 7, 233: 7,  # enlargement
            311: 8, 312: 8, 313: 8,  # dissection
            321: 9, 322: 9, 323: 9,  # frag per se
            331: 10, 332: 10, 333: 10  # creation
            }

            # Typology labels for new values
            typology_labels = {
            #0: "background", 
            1: "Attrition", 
            2: "Aggregation", 
            3: "Shrinkage",
            4: "Perforation", 
            5: "Deformation", 
            6: "Persistent", # originally was shift 
            7: "Enlargement",
            8: "Dissection", 
            9: "Fragmentation per se", 
            10: "Creation"
            }
            
            ########## need to set anything not covered by the recode map to 0 and preserve current nodata values
            # Create remap - unmapped values become NODATA
            remap_rules = [[old_values, old_values, new_values] for old_values, new_values in recode_map.items()]
            remap = arcpy.sa.RemapValue(remap_rules)
            
            # reclassification
            output_raster = arcpy.sa.Reclassify(input_raster, "Value", remap, "NODATA")
            output_raster.save(output_path)
            
            # Build raster attribute table
            arcpy.management.BuildRasterAttributeTable(output_path)
            
            # Add typology field
            arcpy.management.AddField(output_path, "TYPOLOGY", "TEXT", field_length=50) # adds typology name field

            # Add km2 field
            year2 = get_year(basename)
            km2_field = f"km2_{year2}".replace("-", "_")  # Replace dash with underscore
            arcpy.management.AddField(output_path, km2_field, "DOUBLE") # adds km

            # Update field with cursor
            with arcpy.da.UpdateCursor(output_path, ["Value", "Count", "TYPOLOGY", km2_field]) as cursor:
                for row in cursor:
                    if row[0] in typology_labels:
                        row[2] = typology_labels[row[0]] # typology field #[1]
                        row[3] = row[1] * 0.0009 # year_km2 field = count field * 0.0009 = km2
                        #row[3] = round(row[1] * 0.0009)  # Round to whole number
                        cursor.updateRow(row)
            
            print(f"Typology reclassification successful: {output_path}")
            
        except Exception as e:
            print(f"Typology reclassification error for {combined_file}: {str(e)}")


### Main Execution ###
if __name__ == "__main__":
    print("Starting Processing")
    
    # Debug: Check paths
    # print(f"Input raster: {input_raster}")
    # print(f"Input exists: {os.path.exists(input_raster)}")
    # print(f"Output dir: {output_dir}")
    # print(f"Output dir exists: {os.path.exists(output_dir)}")
    
    # Debug: Check ArcPy
    # try:
    #     print(f"ArcPy version: {arcpy.GetInstallInfo()['Version']}")
    #     arcpy.CheckOutExtension("Spatial")
    #     print("Spatial Analyst license checked out")
    # except Exception as e:
    #     print(f"ArcPy setup error: {e}")
    #     sys.exit(1)
    
    ## Remap Raster - Time Series
    # print("Starting remapping process...")
    # rmp_start = time.time()
    # rmp_results = remap_time_series(
    #     input_dir=os.path.dirname(input_raster),
    #     output_dir=output_dir,
    #     metric=metric_type
    # )
    # rmp_duration = time.time() - rmp_start
    # print(f"Remap completed in {rmp_duration:.2f} seconds")
    
    ## Remap Raster - Time Interval
    # print("Starting remapping process...")
    # rmp_start = time.time()
    # rmp_results = remap_time_interval(
    #     input_dir=os.path.dirname(input_raster),
    #     output_dir=output_dir,
    #     metric=metric_type
    # )
    # rmp_duration = time.time() - rmp_start
    # # print(f"Remap completed in {rmp_duration:.2f} seconds") # just prints seconds
    # print("Remap completed in {:.0f} mins. {:.2f} sec.".format(rmp_duration // 60, rmp_duration % 60))
    
    ## Combine Rasters
    # print("Starting combining process...")
    # c_start = time.time()
    # c_results = combine_by_year(
    #     input_dir= combine_input,
    #     output_dir= combine_output
    # )
    # c_duration = time.time() - c_start
    # print("Combine completed in {:.0f} mins. {:.2f} sec.".format(c_duration // 60, c_duration % 60))
    
    ## Reclassify Combined Raster and Add Typology Names
    print("Starting reclassification process...")
    rc_start = time.time()
    reclassify_typology(
        input_dir= rc_input,
        output_dir= rc_output
    )
    rc_duration = time.time() - rc_start
    print("Reclassification completed in {:.0f} mins. {:.2f} sec.".format(rc_duration // 60, rc_duration % 60))
