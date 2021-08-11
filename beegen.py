#!/usr/bin/python3

import re
import os

# Windows/laptop path
# C:\Users\myuser\OneDrive\Documents\GitHub\tools\beegen



def read_template_file(myfile):
    """
    Reads myfile which contains a BEE sql template, returns string
    """
    with open(myfile,'r') as inputFile:
        templateList = inputFile.readlines()
    return ''.join(templateList)


def read_control_file(myfile,sep):
    """
    Reads a control (data) file with the information listed, separated by {sep} separator,
    and returns a list of tuples
    """
    with open(myfile, 'r') as inputFile:
        controlDataList = inputFile.readlines()
    finalList = []
    for line in controlDataList:
        finalList.append((line.split(sep)[0].strip(),line.split(sep)[1].strip(),line.split(sep)[2].strip()))
    return finalList

def replace_values(mytemplate,changesTuple):
    """
    Finds _XXX_ and XXXXXXXXXX in a file and replaces them with
    changesTuple[0] and changesTuple[1] respectively
    """
    previousTag = read_tag_file()
    currentTag = previousTag + 1


    for item in changesTuple:
        if "RPXXX_" in mytemplate:
            mytemplate = re.sub("RPXXX_","RP"+changesTuple[0]+"_",mytemplate)
        if "_DRSXXXD_" in mytemplate:
            mytemplate = re.sub("_DRSXXXD_","_DRS"+changesTuple[0]+"D_",mytemplate)
        if "_XXX_" in mytemplate:
            mytemplate = re.sub("_XXX_","_"+changesTuple[1]+"_",mytemplate)
        if "XXXXXXXXXX" in mytemplate:
            mytemplate = re.sub("XXXXXXXXXX",changesTuple[2],mytemplate)
        if "INSERT_LIQUIBASE_TAG_HERE" in mytemplate:
            mytemplate = re.sub("INSERT_LIQUIBASE_TAG_HERE","--changeset ibfgdl01:mav-jira00-snowflake-"+str(currentTag), mytemplate)
    write_tag_file(currentTag)
#    print(mytemplate)
    return mytemplate

def output_to_file(string,filename,directory):
    """
    Takes string, and writes it to directory/filename
    """
    fullPath = directory+"/"+filename
    outputFile = open(fullPath,'w')
    outputFile.write(string)
    outputFile.close()


def read_tag_file():
    """
    Reads LAST_USED_TAG.txt and returns an integer of the most recent number at the end
    Current format of the tag is:
    --changeset ibfgdl01:mav-jira00-snowflake-1
    In this case, this function will return "1" - the last number.
    """
    tag_file = open("LAST_USED_TAG.txt",'r')
    fileContents = tag_file.readline()
    tag_file.close()
    return int(fileContents.split('-')[5].strip())



def write_tag_file(num):
    """
    Overwrites the tag file LAST_USED_TAG.txt with the most recent number
    used in sql generation. This is used for liquibase deployments
    """
    tag_file = open("LAST_USED_TAG.txt",'w')   # Note NOT append
    tagPrefix = "--changeset ibfgdl01:mav-jira00-snowflake-"
    tag = tagPrefix + str(num)
    tag_file.write(tag)
    tag_file.close()
    

if __name__ == "__main__":
    os.chdir("C:\\Users\\myuser\\OneDrive\\Documents\\GitHub\\tools\\beegen")
    #templatefile = "NWIRPXXX_DRSXXXD_XXX_DELETE_PREP_SNOWFLAKE.sql"
    #templatefile = "NWIRPXXX_DRSXXXD_XXX_DELETE_SNOWFLAKE.sql"
    #templatefile = "NWIRPXXX_DRSXXXD_XXX_LOAD_SNOWFLAKE.sql"
    #templatefile = "NWIRPDUP_DUPCHKD_XXX_DUPLICATE_CHECK.sql"
    templatefile = "NWIRPTRG_TRGD_XXX_CHECK_COUNTS.sql"
    #templatefile = "NWIRPXXX_XXX_.esg"
    controlfile = "BEE_MAPPING.txt"
    fileSep = "|"
    outputDirectory = "output"
    myTemplate = read_template_file(templatefile)
    myControl = read_control_file(controlfile,fileSep)
    for pair in myControl:
        myFileName = templatefile  # For start, need to replace _XXX_
        myFileName = re.sub("_XXX_","_"+pair[1]+"_",myFileName)
        myFileName = re.sub("RPXXX","RP"+pair[0],myFileName)
        myFileName = re.sub("DRSXXXD","DRS"+pair[0]+"D",myFileName)
        print("Generating sql for "+myFileName)
        myFile = replace_values(myTemplate,pair)
        output_to_file(myFile,myFileName,outputDirectory)