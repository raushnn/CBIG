import os
from datetime import datetime, tzinfo, timedelta



# ==============================================COMMON SETTINGS===========================================================================================================
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sftpauth = {
    "sftphostkey" : r'C:\Users\khannapo\.ssh\known_hosts'
}
chromedriver = {
    "Chrome": ROOT_DIR+"\\chromedriver\chromedriver_win32/chromedriver"
}
browser = {
    "BROWSER": "Chrome"
}
DocumentdownloaddirectoryChrome = {
    "DownloadPath": ROOT_DIR+"\\DownloadFiles\\"
}

SMTPServer = {
    "External_SMTP": "smtp-gw.us.dnb.com",
    "Port": "25"
}

mysql = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    "SERVER": "10.252.5.10",
    "DATABASE": "InHouseData",
    "UID": "shethd",
    "PWD": "Password@123"
}

FailedPDFS = {
    "FailedPDFS": ROOT_DIR+"\\OutputFiles\\FailedPDFs\\"
}

XMLOutPutFile = {
    "XMLOutPutFile": ROOT_DIR+"\\InputFiles\\XMLs\\"
}

AOCJobName = {
    "Job": "NonXBRLAOC4"
}


DIRJobName = {
    "Job": "DIR12"
}
# ==============================================AOC SETTINGS===========================================================================================================
InPutFileAOCNonXBRL = {
    "InPutFileAOC4": ROOT_DIR+"\\InputFiles\\AOC4\\"
}
LogFileAOC = {
    "LogFileAOC": ROOT_DIR+"\\LogFiles\\AOC4\\"
}
TrackFileDownload = {
    "TrackFileDownload": ROOT_DIR+"\\AdditionalFiles\\TrackAOCFileDownload.txt"
}
AOC4Template = {
    "AOC4Template": ROOT_DIR+"\\AdditionalFiles\\AOC4\\"
}
OutPutFile = {
    "OutPutFile": ROOT_DIR+"\\OutputFiles\\PDFs\\AOC4\\"
}
# ======================================================================================================

LogFileDIR12 = {
    "LogFileDIR12": ROOT_DIR+"\\LogFiles\\DIR12\\"
}
InPutFileDIR12 = {
    "InPutFileDIR12": ROOT_DIR+"\\InputFiles\\DIR12\\"
}
OutPutFileDIR12 = {
    "OutPutFileDIR12": ROOT_DIR+"\\OutputFiles\\PDFs\\DIR12\\"
}

# ===========================================================================================================================================
InPutFileForm8 = {
    "InPutFileForm8": ROOT_DIR+"\\form8\\"
}
PorcessedPDFS = {
    "PorcessedPDFS": ROOT_DIR+"\\ProcessedPDFs\\"
}
ImagePath = {
    "ImagePath": ROOT_DIR+"\\img\\"
}

# ======================================Financial Upload======================================

InPutFilesFinancial = {
    "InPutFilesFinancial": ROOT_DIR+"\\InputFiles\\FinancialFiles\\"
}
LogFileFinancial = {
    "LogFileFinancial": ROOT_DIR+"\\LogFiles\\FinancialFiles\\"
}

OutPutFileFinancial = {
    "OutPutFileFinancial": ROOT_DIR+"\\OutputFiles\\Financial\\"
}

AutoFinUploadJob = {
    "Job": "AutoFinUploadJob"
}

Yesterday = {
    "Yesterday": (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

}

SevenDaysOldFile = {
    "SevenDaysOldFile": (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

}
Today = {
    "Today": (datetime.now()).strftime('%Y-%m-%d')

}

ccamfin_env={"Env":"sand".lower()}

if ccamfin_env["Env"]=="sand":

    ftp = {"hostName":"mftweb.dnb.com","userName":"BLT356_test","pswd":"DUNS@1b2c3z4"}
    TokenDictCCAMFinUpload = {"FinsUpload_Fiscal":"1af3acf73c194eceb2d11bb962fc2a2e",
                                "FinsUpload_PLTMPL":"e16ca3de6ab4462eb35530aa3f93f58e",
                                "FinsUpload_Cashflow":"b2706409a8424f2ab8d6988db8bbf8a2",
                                "FinsUpload_BSTMPL_Section":"b81e9f40d95341f2a7678be8cb71b3c7"
                                }
elif ccamfin_env["Env"]=="prod":
    # ftp = {"hostName":"mft.dnb.com","userName":"UBLT356IT","pswd":"P@ssW0rd@1234"}
    ftp = {"hostName": "mft.dnb.com", "userName": "UBLT356IT", "pswd": "P@ssW0rd@1234"}
    TokenDictCCAMFinUpload = {"general_non_loop_All":"be95a6d41e014029a914bde160b6970",
                                "General_EXIM_Loop_All":"838a19168cb14032bb3bcddcca136d4",
                                "CMPL2_MCA_Looping_All":"d3a964c2b3ff460ea78864053950b12",
                                "Relcon_SUBS_Looping_All":"f46d96dc12524d29b2c85df05350d39"
                                }
# ======================================Non Financial Upload======================================
InPutFilesNonFinancial = {
    "InPutFilesNonFinancial": ROOT_DIR+"\\InputFiles\\NonFinancialFiles\\"
}
LogFileNonFinancial = {
    "LogFileNonFinancial": ROOT_DIR+"\\LogFiles\\NonFinancialFiles\\"
}

OutPutFileNonFinancial = {
    "OutPutFileNonFinancial": ROOT_DIR+"\\OutputFiles\\NonFinancial\\"
}

AutoNonFinUploadJob = {
    "Job": "AutoNonFinUploadJob"
}
# FTP Details


# ======================================Report to DEWS Config details======================================

InPutFilesReportToDews = {
    "InPutFilesReportToDews": ROOT_DIR+"\\InputFiles\\ReportToDews\\"
}
LogFileReportToDews = {
    "LogFileReportToDews": ROOT_DIR+"\\LogFiles\\ReportToDews\\"
}

OutPutFilesReportToDews = {
    "OutPutFilesReportToDews": ROOT_DIR+"\\OutputFiles\\ReportToDews\\"
}


multi ={
    
    'delete' : ROOT_DIR+"\\OutputFiles\\ReportToDews\\multicsv_delete",
    'nonDelete' : ROOT_DIR+"\\OutputFiles\\ReportToDews\\multicsv_nondelete"
    
    
    }

ReportToDewsJob = {
    "Job": "AutoFinUploadJob"
}

ThreeMonthsOldFile = {
    "ThreeMonthsOldFile": (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
}

iaccess_env={"Env":"sand".lower()}

if iaccess_env["Env"]=="sand":

    iAccess = {
                "DRIVER": "{ODBC Driver 17 for SQL Server}",
                "SERVER": "10.252.4.85",
                "DATABASE": "CompanySearch",
                "UID": "reporttodews",
                "PWD": "pass@1234"
}

elif iaccess_env["Env"]=="prod":

    iAccess = {
                "DRIVER": "{ODBC Driver 17 for SQL Server}",
                "SERVER": "10.252.193.10",
                "DATABASE": "CompanySearch",
                "UID": "reporttodews",
                "PWD": "Reporttodews@123"
}
cbignonfin_env={"Env":"sand".lower()}



emailreceipient = {
    "emailreceipient" : 'khannapo@dnb.com'
}

InPutFilesGDONonFinUpload = {
    "InPutFilesGDONonFinUpload": ROOT_DIR+"\\InputFiles\\GDONonFinFiles\\"
}
LogFileGDONonFinUpload = {
    "LogFileGDONonFinUpload": ROOT_DIR+"\\LogFiles\\GDONonFinFiles\\"
}

OutPutFilesGDONonFinUpload = {
    "OutPutFilesGDONonFinUpload": ROOT_DIR+"\\OutputFiles\\GDONonFinFiles\\"
}

gdononfin_env={"Env":"sand".lower()}

if gdononfin_env["Env"]=="sand":

    ftp = {"hostName":"mftweb.dnb.com","userName":"BLT356_test","pswd":"DUNS@1b2c3z4"}
    TokenDictGDONonFinUpload = {"general_non_loop_All":"ea69b08b6d414c769e0b24e54f574545",
                                "General_EXIM_Loop_All":"77853ff0f471452d8aed47ad95302075",
                                "CMPL2_MCA_Looping_All":"435613efabb84d9f940fb3a91ba3ae9e",
                                "Relcon_SUBS_Looping_All":"58e8b0338e714c3d88e00163385063ae",
                                "Relcon_AFFiliate_Looping_All":"dc2922641ea247b5bd077f52b6030421",
                                "Relcon_Parent_Non_Looping_All":"293787ad940e45b096db790187e9254a",
                                "History_Auditor_Loop_All":"8eafd7284e514f45838df2d9197a967f",
                                "History_Charge_details_Loop_All":"f5f7b40bd0c644e8a66ba7ff186886c1",
                                "History_MCA_Non_Loop_All":"d81a4bb182954968966e7e1b13002125",
                                "History_ROC_Add_Looping_All":"3f37f50d276042f7a4fd40d168a07d90",
                                "History_other_than_ROC_Add_Loop_All":"835a1e83863547a5b033c1af5f5ba04c",
                                "History_Non_Looping_All":"f88b8dc413294ef4a3a5bb2e8de6fce0",
                                "History_Looping_All":"ceeed618806e4768a7b6059c9b2853d4",
                                "History_Sh_Holding_Looping_All":"99790001940042b78f89df48c05c15b1",
                                "MGMT_Director_Sec_Loop_All":"f50d134fb95348dfb1567440b2166ee5",
                                "MGMT_Other_Dir_Details_Loop_All":"1a3dcf0d81ca4e43a444830fe461cca3",

                                "General_EXIM_Loop_All_Delete": "78dad21a36444033894ac1ac40e168fb",
                                "Relcon_SUBS_Looping_All_Delete": "1bacddda940e460385c88a4c58ee3252",
                                "Relcon_AFFiliate_Looping_All_Delete": "1e84097fcb7d4c579aba3bd6d0b17c87",
                                "History_Auditor_Loop_All_Delete": "98039fb44bab4c239d8efc7b5f51bc4a",
                                "History_Charge_details_Loop_All_Delete": "6c1e96bbbc47403c8e2fd4c8161364ef",
                                "History_ROC_Add_Looping_All_Delete": "dd9d80d503d742429f5f7f1d9298f0c6",
                                "History_other_than_ROC_Add_Loop_All_Delete": "b69306018b124e45a4cef885c12e5732",
                                "History_Looping_All_Delete": "15b86fa6d306499799d06266b48f365e",
                                "History_Sh_Holding_Looping_All_Delete": "e99c44d209a044e9a3b1926ad7ede9a5",
                                "MGMT_Director_Sec_Loop_All_Delete": "896237b5f7954ad28b968b812ae5e012"
                                }
elif gdononfin_env["Env"]=="prod":
    # ftp = {"hostName":"mft.dnb.com","userName":"UBLT356IT","pswd":"P@ssW0rd@1234"}
    ftp = {"hostName": "mft.dnb.com", "userName": "UBLT356IT", "pswd": "P@ssW0rd@1234"}
    TokenDictGDONonFinUpload = {"general_non_loop_All":"be95a6d41e014029a914bde160b69701",
                                "General_EXIM_Loop_All":"838a19168cb14032bb3bcddcca136d4b",
                                "CMPL2_MCA_Looping_All":"d3a964c2b3ff460ea78864053950b125",
                                "Relcon_SUBS_Looping_All":"f46d96dc12524d29b2c85df05350d39d",
                                "Relcon_AFFiliate_Looping_All":"08cf00ed4d6146e0bc514eb9882b8ea3",
                                "Relcon_Parent_Non_Looping_All":"f9da5f75d91f412282664f7610cecf84",
                                "History_Auditor_Loop_All":"6d8812f8eb6f410a9c0f204ea831b7b6",
                                "History_Charge_details_Loop_All":"1e3dcfb3f01642bfa97193c60d8bdf12",
                                "History_MCA_Non_Loop_All":"5c09eb758a814a9db01f3d13644334c2",
                                "History_ROC_Add_Looping_All":"e5ad5c5620454ba3b2c2eeed6d472644",
                                "History_other_than_ROC_Add_Loop_All":"fa77a6a8714745f8996e49291171a532",
                                "History_Non_Looping_All":"421c06aca5ba463eb34efcfd10310f5a",
                                "History_Looping_All":"d5880902cec14130b9c9cfcb1cb49be8",
                                "History_Sh_Holding_Looping_All":"2ce91b670a0b405b91af5a8222f9b3e0",
                                "MGMT_Director_Sec_Loop_All":"f01ceacbd2ad4e128b00e5b35c87a483",
                                "MGMT_Other_Dir_Details_Loop_All":"c92066a38c1242248a3090126482fb73",

                                "General_EXIM_Loop_All_Delete":"3ea64947013241f59786be4e6e9ee8e0",
                                "Relcon_SUBS_Looping_All_Delete":"19c83b5d6b044930b1f7c73c92c17814",
                                "Relcon_AFFiliate_Looping_All_Delete":"703a4ccc276a48878bde9b765387bdb4",
                                "History_Auditor_Loop_All_Delete": "6436b6ffeb0740cc9a5891182dc2ad17",
                                "History_Charge_details_Loop_All_Delete": "0f5c0a5387ec457594573f45328a41ce",
                                "History_ROC_Add_Looping_All_Delete": "aa88cf8adc8b4280990980e266cf45d6",
                                "History_other_than_ROC_Add_Loop_All_Delete": "b3f842e7a98c4a2eb7dda7499bd2e336",
                                "History_Looping_All_Delete": "d3fa3e74a7ee498da4aaf2b80e40bde2",
                                "History_Sh_Holding_Looping_All_Delete": "217c345a038246248a6d6cb2fbd90dca",
                                "MGMT_Director_Sec_Loop_All_Delete": "3dc26e4f072441cd8b075c127ada92a6",

                                }

