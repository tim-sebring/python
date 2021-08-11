#!/usr/bin/python3
import os
import paramiko
import getpass
from cryptography.fernet import Fernet

import smtplib
from email.mime.text import MIMEText

def convert_list_to_string(myList):
    newString = ""
    for item in myList:
        newString += item
    return newString

MailServer = "mail-server.example.net"
MailPort = 25

esplmi = "/usr/local/ESP/Agent/ESPlmi"
linux_host = "linux_host"

#userid = input("Enter your User ID: ")
userid = "generic"

#passwd = getpass.getpass()
encrypted_password = b"binary_encrypted_password"

keyfile = open("key.key","rb")
key = keyfile.read()
keyfile.close()

f = Fernet(key)

decrypted_password = f.decrypt(encrypted_password).decode()
passwd = decrypted_password

ssh_connection = paramiko.SSHClient()
ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_connection.connect(linux_host)


applListFile = "/home/myuser/repos/tools/lmi/applfile.txt"
#applListFile = "/home/myuser/repos/tools/lmi/testfile.txt"

applList = []

with open(applListFile,'r') as ALF:
    applList = ALF.readlines()

# TODO: Add progress indicator either with .'s, *'s, or the appl name it's searching

noHomeJobs = []
    
for appl in applList:
    LMICommand = "lapx "+appl+" blocking;"
    command = "/usr/local/ESP/Agent/ESPlmi " \
        "-smainframe_hostname:5301 " \
        "-u"+userid+" -p"+passwd+" " \
        "'-c"+LMICommand+"'"

    stdin, stdout, stderr = ssh_connection.exec_command(command)
    output = ""
    stdout=stdout.readlines()

    # Strip whitespace from the list items
    newList = []
    for line in stdout:
        newItem = line.strip()
        newList.append(newItem)

    for line in newList:
        if("NO HOME JOB FOUND" in line) and ("_PT1" in line):
            noHomeJobs.append(line+"\n")

# Remove duplicate entries in the list
noDupes = []
noDupes = list(set(noHomeJobs))


debug = False
            
if(len(noDupes) > 0 or debug):
    # Convert from list to string
    sNoHomeJobs = convert_list_to_string(noDupes)
    if(debug):
        sNoHomeJobs += "\nEnd of message"
    msg = MIMEText(sNoHomeJobs)
    msg['Subject'] = "NO HOME JOBS FOUND in PT1"
    msg['From'] = "Department Automation"
    if(debug):
        msg['To'] = "myuser@mycompany.com"
    else:
        msg['To'] = "ESP_No_Home_Jobs@mycompany.com"
    mailServer = smtplib.SMTP(MailServer,MailPort)
    mailServer.send_message(msg)
    mailServer.quit()
