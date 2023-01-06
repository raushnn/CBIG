from datetime import datetime
from CBIG.CBIGUtilities import AutoReportToDews
import time
import os

from CBIG.CCAMUtilities import CCAMFinUpload
from CBIG.SharepointSettings import settings_ReportToDews
from CBIG.DatabaseConfigFile import *
from CBIG.SetLogger import Logs
from CBIG.cbig_data_validation import validate_the_data
# Define time to set in the log file name
today = datetime.today().strftime('%Y-%m-%d')
log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Log files, paths and setup
loggerObj = Logs()
log_file_name = "CCAM Financial Upload" + today + ".txt"
log_stats_path = settings_ReportToDews.get('team_site_url') + '/Shared%20Documents/CCAM/Logs_Stats/'
log_path = settings_ReportToDews.get('team_site_url') + '/Shared%20Documents/CCAM/Logs_Stats/'
logger = loggerObj.setup_logger('ReportToDewsLogs_',  LogFileReportToDews["LogFileReportToDews"] + log_file_name)

# Code starting point
logger.info("*********************************START*****************************************************")
process_start_time = time.time()
logger.info("Process Started " + str(datetime.now().strftime('%d_%m_%Y %H_%M_%S')))

Inputdirectory = InPutFilesFinancial["InPutFilesFinancial"]
Outputdirectory = OutPutFileFinancial["OutPutFileFinancial"]
if not os.path.exists(Inputdirectory):
    os.makedirs(Inputdirectory)



# Check the output folder and delete if any '.csv' files exists
logger.info("Deleting Existing Csv files from output folder " + str(datetime.now().strftime('%d_%m_%Y %H_%M_%S')))
for i in os.listdir(Outputdirectory):
    if '.csv' in i:
        os.remove(Outputdirectory + '\\' + i)
logger.info("Files Deleted from output folder " + str(datetime.now().strftime('%d_%m_%Y %H_%M_%S')))

Obj1 = CCAMFinUpload(logger)
folder_url = '/sites/ReportToDews/Shared Documents/CCAM/ProcessedFinCSVFiles/'
deleted = Obj1.delete_file(folder_url)


download_folder_url = '/sites/ReportToDews/Shared Documents/CCAM/SourceFiles/Input_Financial'
downloaded = Obj1.download_file(download_folder_url)


# loop through the input directory and Validating the file,  if not validated then delete it
#Obj1.validate_input_files(Inputdirectory)

# # Validate the data
#validate_the_data()
# #
#Upload Files
#Obj1.upload_file('/sites/ReportToDews/Shared Documents/CBIG/Processed_Files/', '', 'CSV', logger)
# #
# # # Upload files to FTP
obj1.delete()
#sOutputdirectory = OutPutFilesReportToDews["OutPutFilesReportToDews"]
#Obj1.uploadt_files_to_ftp(Outputdirectory, logger)
# #
# # process_end_time = time.time()
# # total_elapsed_time = time.strftime("%H:%M:%S %Z",  time.gmtime(process_start_time - process_end_time))
# #
# #
# #
# #
