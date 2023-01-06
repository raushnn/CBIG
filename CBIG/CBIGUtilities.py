from tkinter import EXCEPTION
import pandas as pd
import os
import shutil
from datetime import datetime
import time
from SharepointSettings import settings_ReportToDews
from DatabaseConfigFile import *
import traceback
import pysftp as pysftp
import openpyxl
import paramiko
from pathlib import Path
import numpy as np
import pyodbc
from cbig_data_validation import update_val_in_FinNonFin , column_name_finder

from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from concurrent.futures import ThreadPoolExecutor
import ftplib


class AutoReportToDews :
	def __init__(self , logger) :
		self.logger = logger
		cc = ClientContext (settings_ReportToDews.get ('team_site_url'))
		self.ctx = cc.with_credentials (ClientCredential (settings_ReportToDews[ 'client_credentials' ][ 'client_id' ] ,
		                                                  settings_ReportToDews[ 'client_credentials' ][
			                                                  'client_secret' ]))
		
		logger.info ("Files Deleted from output folder " + str (datetime.now ( ).strftime ('%d_%m_%Y %H_%M_%S')))
	
	def delete_file(self , folder_url) :
		# print(folder_url)
		try :
			# folder_url = '/Shared Documents/CBIG/Processed_Files/'
			# ctx = self.ctx
			files = self.ctx.web.get_folder_by_server_relative_url (folder_url).files
			print ('Files' , files)
			self.ctx.load (files)
			self.ctx.execute_query ( )
			# print('Authenticated into sharepoint as: ', self.ctx.web.properties['Title'])
			
			for file , i in zip (files , range (0 , len (files))) :
				try :
					TimeCreated = (str (file.properties[ 'TimeCreated' ]).split ('T')[ 0 ])
					TimeCreated = datetime.strptime (TimeCreated , '%Y-%m-%d')
					YesterdayDate = datetime.strptime (Yesterday[ "Yesterday" ] , '%Y-%m-%d')
					todaydate = datetime.strptime (Today[ "Today" ] , '%Y-%m-%d')
					
					duration = (todaydate - TimeCreated).days
					# Change the value from 7 to 30 days in production
					if duration > 7 :
						print (file)
						# file.delete_object()
						# self.ctx.execute_query()
						# self.logger.info("CSV File was deleted")
				
				except Exception as ex :
					self.logger.error (str (ex) + traceback.format_exc ( ))
					continue
			
			# return "CSV Files have been deleted"
		#
		except Exception as ex :
			print (ex)
			# self.logger.error(str(ex) + traceback.format_exc())
	
	def download_file(self , folder_url) :
		today = datetime.today ( ).strftime ('%Y-%m-%d')
		log_time = datetime.now ( ).strftime ('%Y-%m-%d %H:%M:%S')
		try :
			
			ctx = self.ctx
			folders = ctx.web.get_folder_by_server_relative_url (folder_url).files
			ctx.load (folders)
			ctx.execute_query ( )
			for s in folders :
				print ('Name: ' , s.properties[ 'Name' ])
			
			FileListCount = [ ]
			t0 = time.time ( )
			try :
				# len(folders)
				for folder , i in zip (folders , range (0 , len (folders))) :
					print (folder)
					FileCoun = 0
					try :
						FolderName = str (folder.properties[ "Name" ])
						print ('FolderName: ' , FolderName)
						IgnoreFolderNames = [ 'Logs_Stats' , 'Processed_Files' ]
						# files = ctx.web.get_folder_by_server_relative_url(
						#     folder_url + FolderName).files
						# if FolderName in IgnoreFolderNames:
						#     continue
						# files = folder.files
						# ctx.load(files)
						# ctx.execute_query()
						
						# print('Files:', files)
						cur_file = folder
						
						# for cur_file_, i in zip(files, range(0, len(files))):
						
						# for i, cur_file in enumerate(files):
						print ('Cur: ' , cur_file)
						FileCoun = FileCoun + 1
						
						download_FileName = os.path.join (InPutFilesReportToDews[ "InPutFilesReportToDews" ] ,
						                                  FolderName)
						file_url = '/sites/ReportToDews/Shared Documents/CBIG/Input_Files/' + FolderName
						
						with open (download_FileName , "wb") as local_file :
							print (';file_url: ' , file_url)
							print ('local_file: ' , local_file)
							try :
								ctx.web.get_file_by_server_relative_url (file_url).download (
									local_file).execute_query ( )
							except Exception as e :
								print (e)
						
						self.logger.info ("File name: {0}".format (
							str (FolderName + "_" + cur_file.properties[ "Name" ])))
						# delete the file from sharepoint
						# Below code To be enabled during production
						# file.delete_object()
						# ctx.execute_query()
						# FileListCount.append(FileCoun)
					except Exception as ex :
						print (ex)
						self.logger.error (str (ex) + traceback.format_exc ( ))
						continue
			
			except Exception as ex :
				print (ex)
				self.logger.error (str (ex) + traceback.format_exc ( ))
		
		
		except Exception as ex :
			print (ex)
			self.logger.error (str (ex) + traceback.format_exc ( ))
			self.logger.error ("SharePoint Folder has not been downloaded")
			return "Folder has not been downloaded"
	
	def upload_file(self , folder_url , log_file , file_type , logger) :
		ctx = self.ctx
		if file_type == "LOGS" :
			path = log_file
			# folder_url = '/Shared Documents/CBIG/Logs_Stats/'
			with open (path , 'rb') as content_file :
				file_content = content_file.read ( )
			target_folder = ctx.web.get_folder_by_server_relative_url (folder_url)
			name = os.path.basename (path)
			target_folder.upload_file (name , file_content)
			ctx.execute_query ( )
		elif file_type == "CSV" :
			
			for file in os.listdir (OutPutFilesReportToDews[ "OutPutFilesReportToDews" ]) :
				print (file)
				try :
					if not file.endswith (".csv") :
						continue
					target_folder = ctx.web.get_folder_by_server_relative_url (folder_url)
					path = os.path.join (OutPutFilesReportToDews[ "OutPutFilesReportToDews" ] , file)
					print (path)
					name = os.path.basename (
						os.path.join (OutPutFilesReportToDews[ "OutPutFilesReportToDews" ] , file))
					with open (path , 'rb') as content_file :
						file_content = content_file.read ( )
					print ('name: ' , name)
					target_folder.upload_file (name , file_content)
					ctx.execute_query ( )
				except Exception as ex :
					print (ex)
					logger.error ("Could not upload the file to csv")
		
		return "File uploaded"
	
	def delete_log_file(self , logger) :
		ctx = self.ctx
		try :
			folder_url = '/Shared Documents/SupremeCourt/AutoFin-NonFinUpload/Logs_Stats/NonFinancialUploadLogs/'
			files = ctx.web.get_folder_by_server_relative_url (folder_url).files
			
			ctx.load (files)
			ctx.execute_query ( )
			
			# len(folders)
			for file , i in zip (files , range (0 , len (files))) :
				try :
					timecreated = (str (file.properties[ 'TimeCreated' ]).split ('T')[ 0 ])
					timecreated = datetime.strptime (timecreated , '%Y-%m-%d')
					sevendaysold = datetime.strptime (SevenDaysOldFile[ "SevenDaysOldFile" ] , '%Y-%m-%d')
					todaydate = datetime.strptime (Today[ "Today" ] , '%Y-%m-%d')
					if timecreated < sevendaysold :
						file.delete_object ( )
						ctx.execute_query ( )
						logger.info ("Log files older than 7 days were deleted")
					# FileListCount.append(FileCoun)
				except Exception as ex :
					logger.error (str (ex) + traceback.format_exc ( ))
					continue
			
			return "Log Files have been deleted"
		
		except Exception as ex :
			logger.error (str (ex) + traceback.format_exc ( ))
	
	def managesqlconnection(self , duns , companyname , logger) :
		
		try :
			# CompanyName=regex.escape(CompanyName, special_only=False)
			sql_conn = pyodbc.connect (
				'DRIVER=' + iAccess[ "DRIVER" ] + ';SERVER=' + iAccess[ "SERVER" ] +
				';DATABASE=' + iAccess[ "DATABASE" ] + ';UID=' + iAccess[ "UID" ] + ';PWD=' + iAccess[
					"PWD" ] + '')
			cursor = sql_conn.cursor ( )
			
			sql2 = """\
            EXEC VerifyDUNSLegalName   @DUNS = ?, @CompanyName = ?;"""
			
			values2 = (str (duns) , companyname)
			
			cursor.execute (sql2 , values2)
			rc = cursor.fetchval ( )
			print (rc)
			sql_conn.commit ( )
			cursor.close ( )
			sql_conn.close ( )
			logger.info ("Data Stored : Process Completed")
			return rc
		except Exception as ex :
			logger.error ("SQL Error")
			logger.error (str (ex))
			return 0
	
	def validate_input_files(self , Inputdirectory) :
		for file in os.listdir (Inputdirectory) :
			filename = Path (os.path.join (Inputdirectory , file))
			filename1 = filename.name
			FileWithoutExtn = os.path.splitext (filename1)[ 0 ]
			Empty = True
			DataMatchesWitUnity = 0
			ErrorMessage = ""
			HistoryNonLoopEmpty = True
			GeneralSourceEmpty = True
			GeneralNonLoopChecked = False
			HistoryNonLoopChecked = False
			GeneralSourceChecked = False
			column_name = column_name_finder (filename)
			try :
				if not file.endswith (".xlsx") :
					continue
				
				self.logger.info ("File In Process : " + filename1)
				# =============================Non Looping=====================================
				IntAndDigits9 = False
				NotEmpty = False
				# Creating dictionary of DataFrames Dynamically and key is the sheet name and value is dataframe
				update_val_in_FinNonFin (filename , self.logger)
				records = pd.read_csv ('FinNonFin2.csv').to_dict (orient = 'records')
				for record in records :
					try :
						key = record[ 'SheetName' ]
						sheet_name = record[ 'SheetName' ]
						use_col = record[ 'CellRange' ]
						df = pd.read_excel (filename , sheet_name = sheet_name ,
						                    index_col = None ,
						                    na_values = [ 'NA' ] ,
						                    usecols = use_col ,
						                    converters = { column_name[ 'DUNS_General' ] : np.int64 })
						
						if key == "General non loop" :
							GeneralNonLoopChecked = True
							if not df.empty :
								# Check if DUNS and Legal Name is not Empty
								Empty = ((pd.isnull (df.loc[ 0 , column_name[ 'DUNS_General' ] ])) and (
									pd.isnull (df.loc[ 0 , column_name[ 'Name_General' ] ]))
								         and (pd.isnull (df.loc[ 0 , column_name[ 'Report base date_General' ] ]))
								         and (pd.isnull (df.loc[ 0 , column_name[ 'Report type_General' ] ])))
								
								# Check if DUNS Number is 9 digit
								df.at[ 0 , 'DUNS_General:aa' ] = int (df.iloc[ 0 , 0 ])
								# df.iloc[0,  0] = str(df.iloc[0,  0])
								Length = len (str (int (df.iloc[ 0 , 0 ])))
								# print(Length[0])
								if Length == 9 :
									IntAndDigits9 = True
								# check if Data Matches with Unity Extract
								DataMatchesWitUnity = self.managesqlconnection (df.loc[ 0 , 'DUNS_General:aa' ] ,
								                                                df.loc[ 0 , 'Name_General:ab' ] ,
								                                                self.logger)
							
							if Empty == True :
								ErrorMessage = " Duns Number or Legal Name or report Base Date or Report Type missing"
							if IntAndDigits9 == False :
								ErrorMessage = ErrorMessage + ",  Duns Number Validation Failed"
							if DataMatchesWitUnity == 0 :
								ErrorMessage = ErrorMessage + ",  Duns Number and Legal Name did not match the Unity Extract"
						
						# Source General: DH
						# Source date General: DI
						# 'Source_General:DH',  'Source date_General:DI',
						elif key == "General_Source" :
							GeneralSourceChecked = True
							if not df.empty :
								# check if DH and DI are not Empty
								GeneralSourceEmpty = ((pd.isnull (df.loc[ 0 , column_name[ 'Source_General' ] ])) and (
									pd.isnull (df.loc[ 0 , column_name[ 'Source date_General' ] ])))
								# print(df.dtypes)
								# Check if Duns Number is Integer and 9 digit in length
							if GeneralSourceEmpty :
								ErrorMessage = ErrorMessage + " , " + column_name[ "Source_General" ] + " or " + \
								               column_name[ "Source date_General" ] + " is Empty"
						
						# Registration type	History:fa
						# Start year	History:kv
						# Control year	History:kw
						# 'Start year_History:kv', 	'Control year_History:kw'
						elif key == "History Non loop" :
							HistoryNonLoopChecked = True
							if not df.empty :
								# check if DUNS and Legal Name is not Empty
								HistoryNonLoopEmpty = (
											(pd.isnull (df.loc[ 0 , column_name[ 'Start year_History' ] ])) and (
										pd.isnull (df.loc[ 0 , column_name[ 'Control year_History' ] ])))
								# print(df.dtypes)
								# Check if Duns Number is Integer and 9 digit in length
							if HistoryNonLoopEmpty :
								ErrorMessage = ErrorMessage + " , " + column_name[ "year_History" ] + " or " + \
								               column_name[ "Control year_History" ] + " is Empty"
						else :
							continue
						
						if (
								GeneralNonLoopChecked == True and GeneralSourceChecked == True and HistoryNonLoopChecked == True) :
							print ('Empty: ' , Empty)
							print ('IntAndDigits9:' , IntAndDigits9)
							print ('DataMatchesWitUnity:' , DataMatchesWitUnity)
							print ('GeneralSourceEmpty: ' , GeneralSourceEmpty)
							print ('HistoryNonLoopEmpty: ' , HistoryNonLoopEmpty)
							
							if (
									Empty == True or IntAndDigits9 == False or DataMatchesWitUnity == 0 or GeneralSourceEmpty == True or HistoryNonLoopEmpty == True) :
								Status = ErrorMessage
								# s.remove(os.path.join(Inputdirectory,  file))
								self.logger.info ("Validation failed")
								os.remove (os.path.join (Inputdirectory , file))
								break
							
							elif (
									Empty == False and IntAndDigits9 == True and DataMatchesWitUnity == 1 and GeneralSourceEmpty == False and HistoryNonLoopEmpty == False) :
								allsheetvalid = True
								Status = 'Validation Successfull'
								# os.remove(os.path.join(Inputdirectory,  file))
								self.logger.info ("Validation Successfull")
								break
					
					except Exception as ex :
						Status = 'Validation Failed'
						
						self.logger.error (filename)
						os.remove (InPutFilesFinancial[ "InPutFilesFinancial" ] + filename)
						self.logger.error (str (ex))
						continue
			
			except Exception as ex :
				print (ex)
				Status = 'Validation Failed'
				
				# self.logger.error(filename)
				# self.logger.error(ex)
				os.remove (os.path.join (Inputdirectory , file))
				continue
		
	def seprate(self) :
		dirname = os.path.dirname (__file__)
		base = dirname.rsplit ('\\' , 1)[ 0 ]
		dir = os.path.join (base , 'OutputFiles\ReportToDews')
		
		
		delete = dir + '\\delete'
		non_delete = dir + '\\nondelete'

		
		if not os.path.exists (delete) :
			os.mkdir (delete)
		
		if not os.path.exists (non_delete) :
			os.mkdir (non_delete)
			
		if not os.path.exists (multi['delete']) :
			os.mkdir (multi['delete'])
		
		if not os.path.exists (multi['nonDelete']) :
			os.mkdir (multi['nonDelete'])
		
		arr = os.listdir (dir)
		
		for i in arr :
			if i.endswith ('csv') and 'Delete' in i :
				try :
					shutil.move (os.path.join (dir , i) , os.path.join (delete , i))
				except :
					pass
			elif i.endswith ('csv') :
				try :
					shutil.move (os.path.join (dir , i) , os.path.join (non_delete , i))
				except :
					pass
	


	def upload_files(self , file , logger,path) :
		
		self.seprate()

		
		# Upload files to FTP
		cnopts = pysftp.CnOpts ( )
		cnopts.hostkeys = paramiko.hostkeys.HostKeys (sftpauth[ "sftphostkey" ])
		hostName = ftp[ "hostName" ]
		userName = ftp[ "userName" ]
		pswd = ftp[ "pswd" ]
		if 'multi' in path:
			try:
				with paramiko.SSHClient ( ) as ssh :
					ssh.load_system_host_keys ( )
					ssh.connect (hostname = hostName , username = userName , password = pswd)
					print('connected')
					sftp = ssh.open_sftp ( )
					sftp.chdir ('puts')
					sftp.put (os.path.join (path , file) ,file)
					# os.remove(os.path.join (path , file) )
				
			except Exception as ex:
				logger.error (str (ex))
				
				
		else:
			try :
				with pysftp.Connection (host = hostName , username = userName ,
				                        password = pswd , cnopts = cnopts) as sftp :
					try :
						logger.info ("Connection established ... ")
						print(path)
						print (userName)
						
						with sftp.cd ("/puts/") :
							try :
								sftp.put (os.path.join (path , file) , confirm = False)
							except Exception as e:
								logger.error(str(e))
						# ,  confirm = False
						time.sleep (60)
						os.remove (os.path.join (path , file))
					except Exception as ex :
						logger.error(str(ex))
			except Exception as ex :
				logger.error (str (ex))
		


	def main(self , logger) :
		self.seprate()
		dirname = os.path.dirname (__file__)
		base = dirname.rsplit ('\\' , 1)[ 0 ]
		dir = os.path.join (base , 'OutputFiles\ReportToDews')
		non_delete = dir + '\\nondelete'
		delete = dir + '\\delete'
		multicsv_delete = dir + '\\multicsv_delete'
		multicsv_nondelete = dir + '\\multicsv_nondelete'
	
	
		delete_file = os.listdir (delete)
		nofiles = os.listdir (non_delete)
		multicsv_delete_files = os.listdir(multicsv_delete)
		multicsv_nondelete_files = os.listdir(multicsv_nondelete)
		
		if len(multicsv_delete_files)>0:
			with ThreadPoolExecutor (max_workers = 5) as executor :
				future_list = [ executor.submit (self.upload_files , file , logger,multicsv_delete) for file in multicsv_delete_files ]
					#data = [ i.result() for i in future_list]

		if len(multicsv_nondelete_files)>0:
			with ThreadPoolExecutor (max_workers = 5) as executor :
				future_list = [ executor.submit (self.upload_files , file , logger,multicsv_nondelete) for file in multicsv_nondelete_files ]
					#data = [ i.result() for i in future_list]



		if len(delete_file)>0:
			with ThreadPoolExecutor (max_workers = 10) as executor :
				future_list = [ executor.submit (self.upload_files , file , logger,delete) for file in delete_file ]
					#data = [ i.result() for i in future_list]
		#
		if len(nofiles)>0:
			with ThreadPoolExecutor (max_workers = 10) as executor :
				future_list = [ executor.submit (self.upload_files , file , logger,non_delete) for file in nofiles ]
				#data = [ i.result() for i in future_list]
		#
		try:
			shutil.rmtree(non_delete)
			shutil.rmtree(delete)
		except:
			pass