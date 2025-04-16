import os, sys
import arcpy
import multiprocessing

# Input parameters
clipper = arcpy.GetParameterAsText(0) if arcpy.GetParameterAsText(0) else r"C:\489\USA.gdb\States"
tobeclipped = arcpy.GetParameterAsText(1) if arcpy.GetParameterAsText(1) else r"C:\489\USA.gdb\Roads;C:\489\USA.gdb\Hydro"
output_fc = arcpy.GetParameterAsText(2) if arcpy.GetParameterAsText(2) else r"c:\489\output"
output_fc = os.path.normpath(output_fc)
tobeclipped_list = tobeclipped.split(";")

def worker(clipper, input_fc, field, oid, output_fc):
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
        tmp_flayer = arcpy.MakeFeatureLayer_management(clipper, f"memory\clipper_{oid}{input_fc}", query)

        # Do the clip. We include the oid in the name of the output feature class.
        input_fc_name = os.path.basename(input_fc).split(".")[0]
        outFC = os.path.join(output_fc, f"clip_{oid}_{os.path.basename(input_fc)}")
        arcpy.Clip_analysis(input_fc, tmp_flayer, outFC)

        # uncomment for debugging
        # arcpy.AddMessage("finished clipping:", str(oid))

        return True # everything went well so we return True
    except:
        # Some error occurred so return False
        # print("error condition")
        return False
def mp_handler():

    try:
        import Testing
        # Create a list of object IDs for clipper polygons
        arcpy.AddMessage("Creating Polygon OID list...")
        clipperDescObj = arcpy.Describe(clipper)
        field = clipperDescObj.OIDFieldName

        idList = []
        with arcpy.da.SearchCursor(clipper, [field]) as cursor:
            for row in cursor:
                id = row[0]
                idList.append(id)

        arcpy.AddMessage(f"Tobeclipped List: {tobeclipped_list}")

        arcpy.AddMessage(f"There are {len(idList)} object IDs (polygons) to process.")

        # Create a task list with parameter tuples for each call of the worker function. Tuples consist of the clippper,
        # tobeclipped, field, and oid values.

        jobs = [(clipper, input_fc, field, oid, output_fc) for oid in idList for input_fc in tobeclipped_list]
        arcpy.AddMessage(jobs)

        arcpy.AddMessage(f"Job list has {len(jobs)} elements.")

        # Create and run multiprocessing pool.
        # Set the python exe. Make sure the pythonw.exe is used for running processes, even when this is run as a
        # script tool, or it will launch n number of Pro applications.
        multiprocessing.set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
        arcpy.AddMessage(f"Using {os.path.join(sys.exec_prefix, 'pythonw.exe')}")

        # determine number of cores to use
        cpuNum = multiprocessing.cpu_count()
        arcpy.AddMessage(f"there are: {cpuNum} cpu cores on this machine")

        # Create the pool object
        with multiprocessing.Pool(processes=cpuNum) as pool:
            arcpy.AddMessage("Sending to pool")
            # run jobs in job list; res is a list with return values of the worker function
            res = pool.starmap((Testing.worker), jobs)

        # If an error has occurred within the workers, report it
        # count how many times False appears in the list (res) with the return values
        failed = res.count(False)
        if failed > 0:
            arcpy.AddError(f"{failed} workers failed!")

        arcpy.AddMessage("Finished multiprocessing!")

    except Exception as ex:
        arcpy.AddError(ex)


if __name__ == '__main__':
    from Testing import worker
    mp_handler()
