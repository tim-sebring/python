#!/usr/bin/python3

import cx_Oracle
import getpass
import sys
import os
import re

def extract_bee_to_list(mycursor,process_name):
    myquery = "SELECT * FROM TABLE (BEE_DBO.BEE_EXTRACT_METADATA.GET_METADATA('"+process_name+"'))"
    results = mycursor.execute(myquery)
    extracted_bee = results.fetchall()  # Returns a list of tuples
    return extracted_bee


def remove_bee_step(mylist,process_name,step_number):
    # Returns new list -- don't modify list you're iterating over!
    myprocess = "'"+process_name+"'"  # We're looking for a name with single quotes around
    string_to_match = "VALUES ("+myprocess+","+str(step_number)+","
    #print("process name = "+process_name+"\nStep# is "+str(step_number)+"\nStM="+string_to_match)

    # Create temporary list
    newlist = []
    for step in mylist:
        if(string_to_match in step[0]):   # got a hit, don't include this step
            pass
        else:      # doesn't match a deleted step, append it to the list
            newlist.append(step)
    return newlist


def write_bee_process_to_file(mylist,bee_process,directory="/tmp/ba/"):

    # mylist is a list of inserts for a single bee process (list)
    # bee_process is the name of the bee_process (string)
    # directory is the output directory, defaults to /tmp (string)
    
    # Ensure directory exists and ends with a /
    if(not os.path.isdir(directory)):
        print("Directory "+directory+" does not exist. Exiting.")
        sys.exit(1)
    else:
        # check to see if directory ends with a /, if not, add one
        if(not directory.endswith("/")):
            directory += "/"


    RENUMBERING = False
    if(RENUMBERING = TRUE):
        new_step_seq = 0   # start with 0, will ignore LOAD_PROCESS_DEFINITIONS also

        # Rather than change it during the write, we'll change it in-place in the list beforehand
        for i, insert in enumerate(mylist):
            if("LOAD_PROCESS_DEFINITION" in insert[0]):
                continue # Skip this one, it's not renumbered
            # Use enumerate to easily replace items in the list
            string_to_replace = r" VALUES \('"+bee_process+"',\d+,"
            if("LOAD_PROCESS_ACTIONS" in insert[0]):
                new_step_seq += 1
                string_to_replace_with = r" VALUES ('"+bee_process+"',"+str(new_step_seq)+","
                newinsert = re.sub(string_to_replace,string_to_replace_with,insert[0])
                mylist[i] = newinsert
            else:
                string_to_replace_with = r" VALUES ('"+bee_process+"',"+str(new_step_seq)+","
                # No need to increment the step, just write it
                newinsert = re.sub(string_to_replace,string_to_replace_with,insert[0])
                mylist[i] = newinsert

    
    with open(directory+bee_process.upper()+'.sql','w') as out:
        for insert in mylist:
            #if insert contains LOAD_PROCESS_ACTIONS then new_step_seq += 1
            #re.replace(insert with "'bee_process'",step_sequence) etc
            out.write("%s\n" % insert)

def connect_to_database():
    userid = input("Enter user ID: ")
    password = getpass.getpass()

    constring = userid+"/"+password+"@tnsaddress.net/mydb.net"
    con = cx_Oracle.connect(constring)
    cursor = con.cursor()
    return cursor


def read_bee_processes_from_file(filename):
    bee_processes = []
    with open(filename) as beelist:
        bee_processes = beelist.read().splitlines()
    return bee_processes


def execute_sql_query(mycursor,query):
    query_results = mycursor.execute(query)
    results_list = query_results.fetchall()
    return results_list


def get_load_process_files_filenames_by_step(mycursor,process_name,step_number):
    """ Returns a list of tuples of filenames interacted by this process/step """
    myquery = "select file_name " \
        "from BEE_DBO.LOAD_PROCESS_FILES " \
        "where process_name = '"+process_name+"' " \
        "and process_step_seq = "+str(step_number)
    mylist = execute_sql_query(mycursor,myquery)
    return mylist

def get_db_id_for_step(mycursor,process_name,step_number):
    # Need to check both LOAD_PROCESS_FILES and LOAD_PROCESS_ACTIONS
    myquery = "select DB_ID from BEE_DBO.LOAD_PROCESS_ACTIONS " \
        "where process_name = '"+process_name+"' " \
        "and process_step_seq = "+str(step_number)+" " \
        "and db_id is not null" 
    lpa_results = execute_sql_query(mycursor,myquery)

    myquery2 = "select DB_ID from BEE_DBO.LOAD_PROCESS_FILES "\
        "where process_name =  '"+process_name+"' " \
        "and db_id is not null"
    lpf_results = execute_sql_query(mycursor,myquery2)
    return lpa_results + lpf_results   # merged lists

def read_steps_to_delete_from_file():
    # accepts file name, reads PROCESS,NUM to delete, populates delete list
    myfile = "/home/myuser/workspace/temp/bee_change_manual.csv"

    with open(myfile,'r') as beelist:
        bee_processes = beelist.read().splitlines()
    delete_steps = []
    for process in bee_processes:
        mytup = (process.split(",")[0],process.split(",")[1])
        delete_steps.append(mytup)

#    for change in delete_steps:
#        newlist = remove_bee_step(process_inserts,change[0],change[1])
#        process_inserts = newlist
    
        
    return delete_steps

###############################################################################################################
if(__name__ == "__main__"):

    cursor = connect_to_database()
    
    bee_process_list = "/home/myuser/repos/tools/bee_analyzer/prod_list_no_drs.txt"

    bee_processes = read_bee_processes_from_file(bee_process_list)

    # Steps with these actions are subject to removal if the file they interact with contains "QN_"
    actions_to_check_for_QN_files = ['EAFTP Delete','Extract Trg','EAFTP Put','Extract Mon',
                                     'EAFTP Get','Immediate UnCompress','Run SQL','SQL LOADER']

    # Steps with these actions are removed with no further checking
    actions_to_check_for_RA = ['Extract Data','Purge Data']

    # Steps with these DB_IDs are subject to removal (connecting to %HUB% databases)
    hub_db_ids_to_check = [13,14,77,78,79,83,85,86,88,89,96]

    # Steps with these DB_IDs are subject to removal (connecting to old netezza)
    onz_db_ids_to_check = [15]  # Removed DB_ID = 10 because some other stuff has it marked but is OOS
    
    step_delete_message = ""  # Forcing global scope
    bee_process_list = []
    step_delete_list = []
    bee_iter = 0
    print("Searching through BEE processes:")    
    for process in bee_processes:
        sys.stdout.write("\r%s\033[K" % str(process))
        sys.stdout.flush()
        # This will loop over every process in the list
        bee_dict = {}
        bee_dict['process_name'] = process
        bee_process_list.append(bee_dict)
        myquery = "select max(process_step_seq) " \
            "from bee_dbo.load_process_actions " \
            "where process_name = '"+process+"'"

        mylist = execute_sql_query(cursor,myquery)

        bee_process_list[bee_iter]['max_steps'] = mylist[0][0]
        #print(str(bee_iter)+": number of steps for "+process+" is "+str(mylist[0])+".")

        myquery = "select * " \
            "from bee_dbo.load_process_actions " \
            "where process_name = '"+process+"'"

        steps = execute_sql_query(cursor,myquery)
        for step in steps:
            myquery = "select action, process_to_run, db_id " \
            "from bee_dbo.load_process_actions " \
            "where process_name = '"+process+"' and " \
            "process_step_seq = "+str(step[1])
            
            find_action = execute_sql_query(cursor,myquery)
            action = find_action[0][0].strip()  # for some reason extra spaces are added -- trim these out
            if find_action[0][1] is not None:
                ptr = find_action[0][1].strip()
            if find_action[0][2] is not None:
                db_id = find_action[0][2]

            # Search for offending DB_IDs
            dbid_steps = get_db_id_for_step(cursor,process,step[1])
            for dbid in dbid_steps:
                # iterate over all found DB_ID entries
                if(dbid in hub_db_ids_to_check or dbid in onz_db_ids_to_check):
                    # add step to delete list
                    step_delete_list.append((process,step[1]))
                    continue  # skip to the next step


            # Make action-related checks
            if(action in actions_to_check_for_QN_files):
                # Check for load_process_files
                files = get_load_process_files_filenames_by_step(cursor,process,step[1])
                if len(files) != 0:
                    # load_process_files entry exists for this process
                    for file in files:
                        if(file[0] is not None and "QN_" in file[0]):
                            # Add this step to the step_delete_list (add a tuple)
                            step_delete_list.append((process,step[1]))
            if(action in actions_to_check_for_RA):
                # No need to search any further, this step is being removed
                step_delete_list.append((process,step[1]))

            if(action == "Execute" and ptr == "bee_netezza_nzload_interface.pl"):
                # Delete this step
                step_delete_list.append((process,step[1]))
            if(action == "Execute" and ptr == "nzload" and db_id == 10):
                # delete this step
                step_delete_list.append((process,step[1]))
            if(action == "Execute" and ptr == "nzload" and db_id == 15):
                # delete this step
                step_delete_list.append((process,step[1]))
            if(action == "Run SQL" and "MYSCHEMA.EXTRACT_UTILITY.REGISTER_DATA_TO_EXTRACT" in ptr and db_id == 84):
                step_delete_list.append((process,step[1]))
            if(action == "Run SQL" and "MYSCHEMA.EXTRACT_UTILITY.REGISTER_DATA_TO_PURGE" in ptr and db_id == 84):
                step_delete_list.append((process,step[1]))
                
            #else:
                #continue # placeholder, alone this command does nothing
            files = None

        # Extract metadata
        process_inserts = extract_bee_to_list(cursor,process)
        # Delete the stuff you don't want


        if(len(step_delete_list) == 0):
            # Nothing will be deleted, write to a diff directory
            write_bee_process_to_file(process_inserts,process,"/home/myuser/workspace/bee_files/no_changes/")
        
        else:
            for change in step_delete_list:
                newlist = remove_bee_step(process_inserts,change[0],change[1])
                process_inserts = newlist
                #TODO: If process_inserts only contains LOAD_PROCES_DEFINITION table, instead
                # spit out a retire-bee-process script.
                # /home/myuser/workspace/bee_files/full_decom
            if len(process_inserts) == 1:
                # There is only one insert, process definition, so this is a total decom
                write_bee_process_to_file(process_inserts,process,"/home/myuser/workspace/bee_files/full_decom/")
            else:
                write_bee_process_to_file(process_inserts,process,"/home/myuser/workspace/bee_files/")
        step_delete_list = [] # Reset this for next bee process
        
        # Increment count for list
        bee_iter +=1

    print("\nProcessing complete.\n")
    