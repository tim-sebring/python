#!/usr/bin/python

import sys
import os
import glob
import ntpath
from datetime import datetime

def createList(filename,tablename):
    """ This takes as input a DRS control file and creates a list that contains a
        list of all of the DSV files in that control file, and returns the list """
    
    # Verify filename exists
    if not os.path.exists(filename):
        print "Error - "+filename+" does not exist! Exiting!"
        sys.exit(1)

    # Read file (should be a single line) into a variable
    # and replace delimiter with newline in one action
    delim = ","   #default delimiter
    with open(filename, 'r') as inputFile:
        data=inputFile.read().replace(delim,'\n')   # replace delimeter with newline

    # Create a list from the data variable, so we can iterate over the list easily and
    # remove all of the path characters (leaving just the filename of the dsv files)
        
    listData = data.split("\n")

    fileList = []  # Create an empty list
    for listItem in listData:
        fileList.append(ntpath.basename(listItem))
        
    return fileList   # Return the filename of the listFile


######################################################################################
# Beginning of main program

if __name__ == "__main__":

    # Start keeping track of how long each part takes
    print str(datetime.now())+" - File validation script executing."

    # Pull environment variable for CFA base path
    try:
        baseCFApath = os.environ['APP_PATH_DIR']
    except KeyError:
        # environment variable not set, print error and die.
        print "Error: Base CFA path is not set! Are you running this under the correct ID?"
        sys.exit(1)

    # Get the rest of the path from the command line options (ie /OOI_DATAFILES/SOMETHING)
    # This will likely be hardcoded to /OOI_DATAFILES/DRS, but we'll see.
    try:
        filepath = sys.argv[1]
    except IndexError:
        print "Error: No command parameter was given. Please enter a relative filepath!"
        sys.exit(1)

    # If the path doesn't already end with a trailing slash, add one
    if not filepath.endswith("/"):
        filepath+="/"

    # Calculate the total path (CFA path + localized path)
    # For example  /devl/ooi_cfa_dev3 = baseCFA Path, filepath may be /OOI_DATAFILES/DRS
    totalpath = baseCFApath + filepath

    # Check to make sure the provided basepath + relative path exists, if not, exit
    if not os.path.isdir(totalpath):
        print "Error: Directory "+totalpath+" doesn't exist! Exiting."
        sys.exit(1)

    # Check to make sure the 2nd command line parameter was the database name ie SECURITY_DETAILS
    # This needs to be done before checking for control files, because the table name is used
    # in searching for the correct control file (per table name)
    try:
        tablename = sys.argv[2] # Look for the 2nd parameter passed to the script
    except IndexError:
        print "Error: No database table name specified! Pass table name as 2nd parameter!"
        sys.exit(1)


    # Check for .temp files (files that were in-process of being transferred to replicat.
    # If any are found, abend immediately because it means replicat did not shut down cleanly.

    numTempFiles = 0
    tempFiles = glob.glob(totalpath+"*"+tablename+"*.temp")
    for n in tempFiles:
        numTempFiles=+1

    # Check to see if any temp files exist
    if numTempFiles > 0:
        print "Error: A .temp file found for table "+tablename+"!"
        print "Replicat did not shut down cleanly! Exiting!"
        print "Temp File(s) Found:"
        print "\n".join(str(item) for item in tempFiles)
        sys.exit(1)

        
    # Look for a control file in the specified directory based on hte tablename we just read in
    numControlFiles = 0
    controlFiles = glob.glob(totalpath+"*"+tablename+"*.control")  # Saved this for use later
    for n in controlFiles:
        numControlFiles=+1

    # Immediately fail if we find more than one control file for a particular table
    if numControlFiles > 1:
        print "Error: Multiple control files found for table "+tablename+"!! Exiting!"
        print "Control File(s) Found:"
        print "\n".join(str(item) for item in controlFiles)
        sys.exit(1)

        
    # If no control file exists for tablename, ensure that no DSV files of that tablename exist
    if numControlFiles == 0:
        rogueDSVFiles = glob.glob(totalpath+"*"+tablename+"*.dsv")
        if rogueDSVFiles:
            # This means that a DSV file from "tablename" was found without a matching
            # control file. This is bad and should be an immediate abend
            print "Error: No control file found for table name "+tablename+", but"
            print "there are corresponding DSV files found:"
            print "File(s):"
            print "\n".join(str(item) for item in rogueDSVFiles)
            sys.exit(1)
        else:  # no control file is found, but no DSV files are found. In this case,
               # print a message and exit gracefully
            print str(datetime.now())+" - No control files or DSV files found for "+tablename
            print "Exiting successfully."
            sys.exit(0)  # TODO: Verify this
            
        
    # Ensure DSV files also exist in the same directory
    numDSVfiles = 0
    for n in glob.glob(totalpath+"*"+tablename+"*.dsv"):
        numDSVfiles=+1

    # If we received control files but no DSV files, this is a problem as well.
    if numDSVfiles == 0 and numControlFiles > 0:
        print "Error: No DSV files (*.dsv) exist in "+totalpath+"! Exiting!"
        sys.exit(1)

    
    # Find the control file specified -- by DB table name passed in as 2nd parameter
    controlFile = glob.glob(totalpath+"*"+tablename+"*.control")    

    
    # Create a list file from the control file found, unless we have to create one
    myList = createList(controlFile[0],tablename)
    myList = filter(None, myList)   # Remove empty strings from list (from newlines in file)
    myList.sort()  # Sort the list alphabetically

    # Cycle through the list to see if there are any DSV files in the directory that
    # are rogue (ie NOT in the list)

    # Get the list of dsv files in the directory for that tablename
    dsvFiles = glob.glob(totalpath+"*"+tablename+"*.dsv")
    dsvFiles = filter(None, dsvFiles) # Remove newlines - shouldn't be any, but need to be
                                      # thorough
    
    dsvFileList = []  # Create an empty list
    for dsvListItem in dsvFiles:
        dsvFileList.append(ntpath.basename(dsvListItem))
    dsvFileList.sort() # Sort the list alphabetically


    # Now we have a list of files that the Replicat server thinks should be there
    # (from the control file), and a list of .dsv files that actually exist in the
    # directory. Subtracting those lists (both ways) will determine if the lists
    # are identical. Any extra filenames should result in an abend

    # calculate files in the control file that don't exist in the directory
    extraMyListFiles = list(set(myList) - set(dsvFileList))  
    if extraMyListFiles:   # If it contains any values -- an empty list is "False"
        print "Error: Control file contains dsv files that do not exist:"
        print "File(s):"
        print "\n".join(str(item) for item in extraMyListFiles)
        sys.exit(1)

    
    # calculate files that exist in the directory that don't exist in the control file
    extraDSVFiles = list(set(dsvFileList) - set(myList))
    if extraDSVFiles:   # If there are any extra dsv files -- an empty list is "False"
        print "Error: Extra DSV Files for "+tablename+" exist that are not in the control file!"
        print "File(s):"
        print "\n".join(str(item) for item in extraDSVFiles)
        sys.exit(1)

    
    print str(datetime.now())+" - File processor complete. Exiting successfully."
    # exit cleanly
    sys.exit(0)