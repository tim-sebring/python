#!/usr/bin/python
############################################################################################
# This script will check /nwiprod/infa to see if the mount is valid. Performing any cmd,   #
# even an "ls" is sufficient to check the return code. Anything other than a zero is       #
# considered a failure, and should cut a ticket, or prompt an automatic remounting of      #
# the resource.                                                                            #
############################################################################################

import os
import sys
import re

path = "/prod/informatica"

result = os.system("ls "+path+" >/dev/null 2>&1")

#print result

# If result is 0, we still need to check to see if it's actually an nfs filesystem (below)
# If result is non-zero, umount and remount the entire share.
# If this fails, there are more issues, and return error for the entire script to cut a ticket

if(result): #nonzero
    mount_cmd = os.popen("which mount").read().rstrip()
    umount_cmd = os.popen("which umount").read().rstrip()

#    print("commands are "+mount_cmd+"\n"+umount_cmd)
    
    umount_result = os.system(umount_cmd+" "+path)
    if(not umount_result):
        # successful umount, attempt remount
        mount_result = os.system(mount_cmd+ " "+path)
        if(not mount_result):
            print("The mount was successfully remounted.")
            sys.exit(0)
        else:
            print("The mount failed with a return code of "+str(mount_result))
            sys.exit(1)
    else:
        print("The umount failed with a return code of "+str(umount_result))
        sys.exit(1)

else: # 0 returned
    # The return code returned 0, but that doesn't mean that the filesystem is
    # mounted. Trying to "ls" an unmounted filesystem will still return a
    # valid 0 code. this next check will make sure its actually an nfs filesystem

    # the following command will return the filesystem type. We're looking for "nfs"
    stat_cmd = "/usr/bin/stat -f -c '%T' "+path

    stat_result = os.popen(stat_cmd)

    if "nfs" in stat_result.read():
        # This is an nfs filesystem, and is mounted cleanly. Success
        print("Success: Filesystem is an NFS filesystem.")
        sys.exit(0)
    else:
        # Not nfs filesystem, so it's not mounted.
        print("Filesystem "+path+" is not an nfs mount, attempting to mount.")
        mount_result = os.system(mount_cmd+ " "+path)
        if(not mount_result):
            print("The mount was successfully remounted.")
            sys.exit(0)
        else:
            print("The mount failed with a return code of "+str(mount_result))
            sys.exit(1)