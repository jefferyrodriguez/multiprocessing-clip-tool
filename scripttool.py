import os, sys
import arcpy
import multiprocessing
from multicode import worker

# Input parameters
clipper = arcpy.GetParameterAsText(0) if arcpy.GetParameterAsText(0) else r"C:\489\USA.gdb\States"
tobeclipped = arcpy.GetParameterAsText(1) if arcpy.GetParameterAsText(1) else r"C:\489\USA.gdb\Roads"
# Creating another parameter that will hold the output folder selected by user. If none is selected, the default is chosen
output_fc = arcpy.GetParameterAsText(2) if arcpy.GetParameterAsText(2) else r"c:\489\output"
output_fc = os.path.normpath(output_fc)

def mp_handler():

    try:
        # Create a list of object IDs for clipper polygons
        arcpy.AddMessage("Creating Polygon OID list...")
        clipperDescObj = arcpy.Describe(clipper)
        field = clipperDescObj.OIDFieldName

        idList = []
        with arcpy.da.SearchCursor(clipper, [field]) as cursor:
            for row in cursor:
                id = row[0]
                idList.append(id)

        arcpy.AddMessage(f"There are {len(idList)} object IDs (polygons) to process.")

        # Create a task list with parameter tuples for each call of the worker function. Tuples consist of the clippper,
        # tobeclipped, field, and oid values.

        jobs = []

        for id in idList:
            # adds tuples of the parameters that need to be given to the worker function to the jobs list
            # The output_fc variable which holds the output folder is added to the tuple for each id in idList. This will be provided to the worker function.
            jobs.append((clipper,tobeclipped,field,id,output_fc))

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
            res = pool.starmap(worker, jobs)

        # If an error has occurred within the workers, report it
        # count how many times False appears in the list (res) with the return values
        failed = res.count(False)
        if failed > 0:
            arcpy.AddError(f"{failed} workers failed!")

        arcpy.AddMessage("Finished multiprocessing!")

    except Exception as ex:
        arcpy.AddError(ex)


if __name__ == '__main__':
    mp_handler()
