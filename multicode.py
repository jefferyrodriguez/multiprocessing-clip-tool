
import arcpy

# The worker function is provided 5 parameters.
# clipper - the feature class that we will be clipping to
# tobeclipped - the feature class we will be clipping
# field - the object id field for each feature in the feature class
# oid - a unique identifer for each member of the tobeclipped feature class
# output_fc - the folder in which the output feature class will be saved

def worker(clipper, tobeclipped, field, oid, output_fc):
    """
       This is the function that gets called and does the work of clipping the input feature class to one of the
       polygons from the clipper feature class. Note that this function does not try to write to arcpy.AddMessage() as
       nothing is ever displayed.
       param: clipper
       param: tobeclipped
       param: field
       param: oid
    """

    try:
        # Create a layer with only the polygon with ID oid. Each clipper layer needs a unique name, so we include oid in the layer name.
        query = f"{field} = {oid}"
        tmp_flayer = arcpy.MakeFeatureLayer_management(clipper, f"memory\clipper_{oid}", query)

        # Do the clip. We include the oid in the name of the output feature class. The output_fc is provided so the output feature class will be saved in the chosen directory
        outFC = f"{output_fc}/clip_{oid}.shp"
        arcpy.Clip_analysis(tobeclipped, tmp_flayer, outFC)

        # uncomment for debugging
        # arcpy.AddMessage("finished clipping:", str(oid))

        return True # everything went well so we return True
    except:
        # Some error occurred so return False
        # print("error condition")
        return False