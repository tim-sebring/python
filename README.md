# python
Just a few of the Python scripts I have written

bee_analyzer.py
- This script will read existing Oracle metadata from a batch orchestration system for 7700+ jobs, determine what jobs need to be updated or deleted based on the decommission of an intermediary database. Update the rules, and re-create the SQL files to create the new job code.

beegen.py
- This script uses a template file to generate SQL code for similar jobs using a control file to substitute job names and other variables. This saved hundreds of hours of development.

file_validation.py
- The purpose of this script is to validate that all files from an Oracle Golden Gate system have been received (checking the list in the control file, to see if all of the trail files have been received, nothing more, nothing less), and verify that all of the downloads have been completed. (no .temp files exist)

nas_mount_monitor.py
- This script would run in cron to monitor stale file handles or unmounted shares, and attempt to automatically resolve the issue by mounting, or unmount/remounting the share.

no_home_job_check.py
- This script would scan pre-production jobs in our ESP environment, looking for any issue of NO HOME jobs (which would cause a production issue if promoted) and notify the developers in advance. Passing this check was a necessary item for production migration in our department.

replicat_control.py
- This script would control the replicat server that managed our Oracle Golden Gate system. It would manage the START, STOP, and INFO commands via command file so that the files could be processed into our target database.

rt.py
- This was just a utility script to convert unix time to standard time, and vice-versa.
