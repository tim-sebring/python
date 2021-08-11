#!/usr/bin/python
################################################################################
# Name            : replicat_control.py
# Description     : Used to control the start, stop, and status features of
#                 : the Golden Gate Data Replication Service, using
#                 : flat-file adapter, preventing data corruption by trying to
#                 : read a file while it's potentially being written to.
#
# Changelog       : Date       Author         Version   Comments
#                   5/9/2018   Tim Sebring    0.01      Initial creation
#                   2/11/2019  Tim Sebring    0.1       Added status check
#
################################################################################

import sys
import os
from datetime import datetime
import subprocess
import time

# set the global filenames so that they are consistent, and avoid scope issues
command_file = ""

# set the path of the command files
filepath = "/tmp/"

# set the Golden Gate Service Command Interpreter (ggsci) file path
ggscipath = "/u01/oracle_11204/app/oracle/product/ogg_flatfile/ggsci"

# Adapter (table group)
adapter = ""

# How long to wait per iteration to check status after a command is issued
statustime = 120  #time in seconds

# How many seconds to wait before sending out "not hung" messages
waittime = 1800  # 30 minutes

# How often to wait before repeating "not hung" messages to console
looptime = 900   # 15 minutes


################################################################################
# This function crudely waits n seconds to facilitate monitoring of the status
# without overloading the GGSCI adapter by issuing commands too quickly
################################################################################
def wait(n):
    """ Waits n seconds. Not super elegant, but gets the job done.
    """
    print("Waiting "+str(n)+" seconds.")
    time.sleep(n)



################################################################################
# This function creates a status file based on the name of the adapter (group
# of tables) specified by the replicat configuration. It checks to see if a
# status is already running (by checking to see if a status file exists) then
# if not, runs. It will delete the status file when complete
################################################################################
def createCommandFile(adapter,command):
    """ Will create a command file with the status/start/stop of the appropriate
        adapter, which includes one or more Eagle tables the adapter will be 
        included in the filename to avoid collisions, in case another adapter 
        process is running at the same time. It will also return the command 
        file name and path for use in execution """

    # Set the statusfile name based on adapter ie status_ffue, start_ffuf, etc
    command_file = command.upper()+"_"+adapter.upper()   
    
    # Ensure there isn't already a process running for this adapter,
    # fail if the file exists.
    if os.path.exists(filepath + command_file):
        print("ERROR: The command file for the existing adapter already exists!")
        print("Another command may be running for this adapter!")
        sys.exit(1)
    else:
        # Create the command file
        fh = open(filepath + command_file, 'w')
        fh.write(command.upper()+" "+adapter.upper()+"\n")
        fh.close()
    fullyPathedFile = filepath + command_file
    return fullyPathedFile


def executeCommandFile(command_file, command):
    """ This method will execute the provided command file on the replicat 
        server. The most used commands are start, stop, and status.""" 
   

    if not os.path.exists(command_file):
        print("ERROR: The command file for the existing adapter does not exist!")
        print("There is nothing to execute!")
        sys.exit(1)
    else:
         # Execute ggscipath with command_file parameter
        myCommand = ggscipath+" < "+command_file+" | grep "+adapter.upper()
        myOutput = os.system(myCommand)

        if myOutput == 0:
            print("Command executed successfully.")
        else:
            print("ERROR: Exit code is "+ str(myOutput))
            sys.exit(1)  
    

def deleteCommandFile(command_file):
    """ This is called to delete the command file that we just executed to keep
        things clean"""
    # Uncomment this out for production
    os.remove(command_file)


def checkStatusBefore(adapter,status):
    """ This function will check the status of the adapter prior to 
        changing anything, to make sure it's already in the correct
        state """

    match = False     # default value
    desiredStatus = status
    myStatusFile = createCommandFile(adapter, "INFO")
    myCommand = ggscipath+" < "+myStatusFile+" | grep "+adapter.upper()+ \
        "| awk '{print $8}'"
    myResult = os.popen(myCommand)
    myStatus = myResult.read().splitlines()[0]
    if myStatus == desiredStatus:
        match = True
    
    deleteCommandFile(myStatusFile)
    return match


def checkStatusAfter(adapter,command):
    """ This function will call the create and execute command file functions
        and use them to determine the status of an adapter. The provided
        adapter variable is the goal state -- return false if the 
        current status does not match the provided status, true if they
        do. Use .upper() to ensure case match. The function will loop until
        the job is killed outside (via ESP dueout) or the status matches
        the desired status. """

#        Possible final statuses are RUNNING, STOPPED, ABENDED. If any other
#        status is received, ignore it and wait again for a final status. This
#        likely means that the adapter is "STOPPING" or "STARTING" and should
#        be ignored for that iteration.

    myStatus = ""
    myStatusFile = createCommandFile(adapter, "INFO")
    myCommand = ggscipath+" < "+myStatusFile+" | grep "+adapter.upper()+ \
        "| awk '{print $8}'"
    myResult = os.popen(myCommand)
    myStatus = myResult.read().splitlines()[0]

    
    start_logging_time = time.time() + waittime   # 30 minutes from now

    logging_mode = False  # Are we printing log messages at this point yet?
    print_log_next_loop = False
    log_loop_time = time.time()   # initialize the variable for scoping

    wait(10)    # wait 10 seconds for initial check    
    # Check again after waiting 
    myResult = os.popen(myCommand)
    myStatus = myResult.read().splitlines()[0]
    
    
    # Do an initial check to see if it matches, if not, enter a wait loop
    if myStatus == command:
        deleteCommandFile(myStatusFile)
        print("Success replicat status for "+adapter.upper()+ \
              " is correct.")
        return True
    
    while myStatus <> command:
        # Start with a sleep, because we just did this above

        print("myStatus is "+myStatus+"\n")
        print("command is "+command+"\n")
        print("myStatusFile is "+myStatusFile+"\n")

            # If status is abended, exit immediately with error
        if myStatus.upper() == "ABENDED":
            print("Error: Adapter "+adapter.upper()+" in ABENDED state!")
            deleteCommandFile(myStatusFile)
            sys.exit(1)

       
        #print("Status does not match command file, sleeping for ")
        #print(statustime+" seconds.\n")            
        wait(statustime)  # wait 2 minutes before checking again

        # Wait for the status to be STOPPED or RUNNING
        myCommand = ggscipath+" < "+myStatusFile+" | grep "+adapter.upper()+ \
            "| awk '{print $8}'"
        myResult = os.popen(myCommand)
        myStatus = myResult.read().splitlines()[0]

        if time.time() > start_logging_time:
            logging_mode = True
        if print_log_next_loop:
            print("Still running, not hung.....\n")
            # Reset loop timer, do it again in 15 minutes
            log_loop_time = time.time() + looptime   # Do this every 15 minutes
            print_log_next_loop = False  # reset this flag
        if logging_mode:
            if time.time() > log_loop_time:
                print_log_next_loop

        # Decision was made to not break out of this loop early, add
        # a dueout or overdue so that a ticket is cut. Otherwise run until
        # the status matches the command, then end gracefully
    # Delete status file
    deleteCommandFile(myStatusFile)
    print("Exiting loop, replicat status for "+adapter.upper()+" is correct.")
    return True
        
def usage():
    """ Prints the usage of the script"""
    print("\nUsage:\nreplicat_control.py <adapter name> <command>\n")
    print("Adapter is the table grouping ")
    print("and command is either \"start\" or \"stop\".\n")
    print("Example:\nreplicat_control.py trns start")
    sys.exit(1)

# Start of main program
if __name__ == '__main__':

    # This script should have two arguments passed to it. The adapter(group)
    # name, and the command. the command is either start or stop. The adapter
    # will be based on the group of tables.

    passedArgs = sys.argv[1:]  # Ignore first arg - it's the script name
    numArgs = len(passedArgs)
    if numArgs != 2:
        usage()
        sys.exit(1)
    else:
        adapter = passedArgs[0]
        command = passedArgs[1]
        myCommandFile = createCommandFile(adapter,command)
        # Check to see if the adapter is already running correctly
        is_error = False
        if command.upper() == "STOP":
            # We want to stop, make sure it's already running
            is_error = not checkStatusBefore(adapter,"RUNNING")
        elif command.upper() == "START":
            # We want to start, make sure it's already stopped
            is_error = not checkStatusBefore(adapter, "STOPPED")
        if is_error:
            if command.upper() == "STOP":
                print("Error! Adapter "+adapter.upper()+" is not RUNNING!")
            elif command.upper() == "START":
                print("Error! Adapter "+adapter.upper()+" is not STOPPED!")
            deleteCommandFile(myCommandFile)
            sys.exit(1)
        else:
            print("Status matches expected result, executing command file.")
        executeCommandFile(myCommandFile,command)
        is_error = False
        if command.upper() == "STOP":
            # Check to make sure it stopped
            is_error = not checkStatusAfter(adapter,"STOPPED")
        elif command.upper() == "START":
            # Check to make sure it started
            is_error = not checkStatusAfter(adapter, "RUNNING")
        if is_error:
            print("Error! After issuing "+command.upper()+", the adapter "+ \
                  adapter.upper()+" is not correct!")
            sys.exit(1)
        else:
            print("Post-execution status matches, deleting command file.")
        
        deleteCommandFile(myCommandFile)

    print("Script terminated normally.")
    sys.exit(0)