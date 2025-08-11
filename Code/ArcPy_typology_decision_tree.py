### Import Packages ###
import os
import arcpy
import arcpy.sa
import multiprocessing
import re
import sys
import time

### Parameters ###
# Remap
# run each metric type separately to remap
input_raster = r"B:\Mikayla\DATA\Projects\AF\Typology_collection9\ETM\31year\area\area_rg_MK_Z.tif"
output_dir = r"B:\Mikayla\DATA\Projects\AF\Typology_collection9\ETM" #"E:\GWB_Working\remap"
metric_type = "area" # or "area", "pn"

# Combine Rasters
# all rasters should be in the same folder- it searches by file name to combine by year
combine_input = r"E:\GWB_Working\remap"
combine_output = r"E:\GWB_Working\combined_output"

# Reclassify and Add Typology Names
rc_input = r"E:\GWB_Working\combined_output"
rc_output = r"E:\GWB_Working\typology_output"

### Functions ###
def get_year(filename):
    # Extract year range pattern like "90-95" from filename (eg. 90-95_area) using regex
    match = re.search(r"(\d{2}-\d{2})", filename)
    return match.group(1) if match else ""
    
def remap_raster(input_dir, output_dir, metric):
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
                # remap_rules = [
                #     (-70, -1.01, 10),
                #     (-1, -0.101, 20),
                #     (-0.1, 0.1, "NODATA"),
                #     (0.101, 1, 20),
                #     (1.01, 70, 30)
                # ]
                remap_rules = [
                    (-70, -1.01, 10),
                    (-1, 1, 20),
                    (1.01, 70, 30)
                ]
                remap = arcpy.sa.RemapRange(remap_rules)
                output_raster = arcpy.sa.Reclassify(input_raster_path, "Value", remap, "NODATA")
            elif metric == "edge":
                output_path = os.path.join(output_dir, f"{year}_edge_rmp.tif")
                # remap_rules = [
                #     (-70, -1.01, 1),
                #     (-1, -0.11, 2),
                #     (-0.1, 0.1, "NODATA"),
                #     (0.11, 1, 2),
                #     (1.01, 70, 3)
                # ]
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
                combined = arcpy.sa.Raster(edge_path) + arcpy.sa.Raster(area_path) + arcpy.sa.Raster(patch_path)
                combined.save(output_path)
                arcpy.management.BuildRasterAttributeTable(output_path)
                print(f"Combined raster created: {output_path}")
            except Exception as e:
                print(f"Combine error: {str(e)}")
                
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
            212: 0,  # background
            111: 1, 112: 1, 113: 1,  # attrition
            121: 2, 122: 2, 123: 2, 131: 2, 132: 2, 133: 2,  # aggregation
            211: 3,  # shrinkage
            213: 4,  # perforation
            221: 5, 223: 5,  # deformation
            222: 6,  # shift                                                                    (ie stable????)
            231: 7, 232: 7, 233: 7,  # enlargement
            311: 8, 312: 8, 313: 8,  # dissection
            321: 9, 322: 9, 323: 9,  # frag per se
            331: 10, 332: 10, 333: 10  # creation
            }

            # Typology labels for new values
            typology_labels = {
            0: "background", 
            1: "attrition", 
            2: "aggregation", 
            3: "shrinkage",
            4: "perforation", 
            5: "deformation", 
            6: "shift", 
            7: "enlargement",
            8: "dissection", 
            9: "frag per se", 
            10: "creation"
            }
            
            # remap rules
            remap_rules = [[old_val, old_val, new_val] for old_val, new_val in recode_map.items()]
            remap = arcpy.sa.RemapValue(remap_rules)
            
            # reclassification
            output_raster = arcpy.sa.Reclassify(input_raster, "Value", remap, "NODATA")
            output_raster.save(output_path)
            
            # Build raster attribute table
            arcpy.management.BuildRasterAttributeTable(output_path)
            
            # Add typology field
            arcpy.management.AddField(output_path, "TYPOLOGY", "TEXT", field_length=50) # adds typology name field

            # Update field with cursor
            with arcpy.da.UpdateCursor(output_path, ["Value", "TYPOLOGY"]) as cursor:
                for row in cursor:
                    if row[0] in typology_labels:
                        row[1] = typology_labels[row[0]]
                        cursor.updateRow(row)
            
            print(f"Typology reclassification successful: {output_path}")
            
        except Exception as e:
            print(f"Typology reclassification error for {combined_file}: {str(e)}")


### Main Execution ###
if __name__ == "__main__":
    print("Starting Processing")
    
    # Debug: Check paths
    print(f"Input raster: {input_raster}")
    print(f"Input exists: {os.path.exists(input_raster)}")
    print(f"Output dir: {output_dir}")
    print(f"Output dir exists: {os.path.exists(output_dir)}")
    
    # Debug: Check ArcPy
    try:
        print(f"ArcPy version: {arcpy.GetInstallInfo()['Version']}")
        arcpy.CheckOutExtension("Spatial")
        print("Spatial Analyst license checked out")
    except Exception as e:
        print(f"ArcPy setup error: {e}")
        sys.exit(1)
    
    ## Remap Raster
    print("Starting remapping process...")
    rmp_start = time.time()
    rmp_results = remap_raster(
        input_dir=os.path.dirname(input_raster),
        output_dir=output_dir,
        metric=metric_type
    )
    rmp_duration = time.time() - rmp_start
    print(f"Remap completed in {rmp_duration:.2f} seconds")
    
    ## Combine Rasters
    # print("Starting combining process...")
    # c_start = time.time()
    # c_results = combine_by_year(
    #     input_dir= combine_input,
    #     output_dir= combine_output
    # )
    # c_duration = time.time() - c_start
    # print(f"Combining completed in {c_duration:.2f} seconds")
    
    ## Reclassify Combined Raster and Add Typology Names
    # print("Starting reclassification process...")
    # rc_start = time.time()
    # reclassify_typology(
    #     input_dir= rc_input,
    #     output_dir= rc_output
    # )
    # rc_duration = time.time() - rc_start
    # print(f"Reclassification completed in {rc_duration:.2f} seconds")
