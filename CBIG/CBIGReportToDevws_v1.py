from datetime import datetime
from CBIG.CBIGUtilities import AutoReportToDews
import time
import os
from CBIG.SharepointSettings import settings_ReportToDews
from CBIG.DatabaseConfigFile import *
from CBIG.SetLogger import Logs
from CBIG.cbig_data_validation1 import validate_the_data
import shutil
# Define time to set in the log file name
today = datetime.today().strftime('%Y-%m-%d')
log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Log files, paths and setup

loggerObj = Logs()
log_file_name = "ReportToDewsLogs_" + today + ".txt"
log_stats_path = settings_ReportToDews.get('team_site_url') + '/Shared%20Documents/CBIG/Logs_Stats/'
log_path = settings_ReportToDews.get('team_site_url') + '/Shared%20Documents/CBIG/Logs_Stats/'
logger = loggerObj.setup_logger('ReportToDewsLogs_',  LogFileReportToDews["LogFileReportToDews"] + log_file_name)

# Code starting point
logger.info("*********************************START*****************************************************")
process_start_time = time.time()
logger.info("Process Started " + str(datetime.now().strftime('%d_%m_%Y %H_%M_%S')))

Inputdirectory = InPutFilesReportToDews["InPutFilesReportToDews"]
Outputdirectory = OutPutFilesReportToDews["OutPutFilesReportToDews"]
if not os.path.exists(Inputdirectory):
    os.makedirs(Inputdirectory)



# Check the output folder and delete if any '.csv' files exists
logger.info("Deleting Existing Csv files from output folder " + str(datetime.now().strftime('%d_%m_%Y %H_%M_%S')))
dirname = os.path.dirname (__file__)
base = dirname.rsplit ('\\' , 1)[ 0 ]
dir = os.path.join (base , 'OutputFiles\ReportToDews')
delete = dir + '\\delete'
non_delete = dir + '\\nondelete'
multicsv_delete = dir + '\\multicsv_delete'
multicsv_nondelete = dir + '\\multicsv_nondelete'

try:
    shutil.rmtree(delete)
    shutil.rmtree(non_delete)
    shutil.rmtree (multicsv_delete)
    shutil.rmtree (multicsv_nondelete)
except:
    pass

# Check the Input folder and delete if any '.xlsx' files exists
logger.info("Deleting Existing Excel files from Input folder " + str(datetime.now().strftime('%d_%m_%Y %H_%M_%S')))
for i in os.listdir(Inputdirectory):
    if '.xlsx' in i:
        os.remove(Inputdirectory + '\\' + i)
logger.info("Files Deleted from input folder " + str(datetime.now().strftime('%d_%m_%Y %H_%M_%S')))

Obj1 = AutoReportToDews(logger)
folder_url = '/sites/ReportToDews/Shared Documents/CBIG/Processed_Files/'
deleted = Obj1.delete_file(folder_url)


download_folder_url = '/sites/ReportToDews/Shared Documents/CBIG/Input_Files/'
downloaded = Obj1.download_file(download_folder_url)


# loop through the input directory and Validating the file,  if not validated then delete it
Obj1.validate_input_files(Inputdirectory)

# # Validate the data
validate_the_data(logger)

#Upload Files
# Obj1.upload_file('/sites/ReportToDews/Shared Documents/CBIG/Processed_Files/', '', 'CSV', logger)
# #
# # # Upload files to FTP
# Obj1.main(logger)

# #
process_end_time = time.time()
total_elapsed_time = time.strftime("%H:%M:%S %Z",  time.gmtime(process_start_time - process_end_time))
# #
# #
# #
# #
