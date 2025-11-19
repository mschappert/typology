import arcpy


# extends the bounding box of a shapefile by a specified distance (in meters)
# this will be axis aligned expansion

# in_fc = r"E:/NSF2_Naoya_Mikayla/Data/nsf2_bbox/nsf2_bbox.shp"
# distance = 1500  # meters

# # Get extent of bounding box
# desc = arcpy.Describe(in_fc)
# extent = desc.extent

# # Expand the extent
# xmin = extent.XMin - distance
# ymin = extent.YMin - distance
# xmax = extent.XMax + distance
# ymax = extent.YMax + distance

# # Create expanded rectangle as a Polygon
# array = arcpy.Array([
#     arcpy.Point(xmin, ymin),
#     arcpy.Point(xmin, ymax),
#     arcpy.Point(xmax, ymax),
#     arcpy.Point(xmax, ymin),
#     arcpy.Point(xmin, ymin)
# ])
# polygon = arcpy.Polygon(array, desc.spatialReference)

# # Save new polygon (use appropriate output path)
# arcpy.CopyFeatures_management([polygon], "E:/NSF2_Naoya_Mikayla/Data/nsf2_bbox/nsf2_bbox_expnd_1.5km.shp")






# Alternative method using Buffer tool with FLAT end type- so the projection remains the same "tilt" as your rasters
in_fc = r"E:/NSF2_Naoya_Mikayla/Data/nsf2_bbox/nsf2_bbox.shp"
distance = 1500  # meters

# Buffer with sharp corners (preserves rectangular shape)
arcpy.analysis.Buffer(in_fc, 
                     "E:/NSF2_Naoya_Mikayla/Data/nsf2_bbox/nsf2_bbox_buff_1.5km.shp", 
                     f"{distance} Meters",
                     "FULL", "FLAT")