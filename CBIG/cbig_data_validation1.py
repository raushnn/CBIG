import shutil

import pandas as pd
import numpy as np
from datetime import datetime
import os
from DatabaseConfigFile import *
from pathlib import Path
import os
from zipfile import ZipFile
import zipfile
import glob
import re
import time


#
# df = pd.read_excel(r"C:\Users\khannapo\Desktop\CBIG\Sample India Limited_ReportToDews_Template.xlsx")
#
# print(df.keys())
# print()
# Data Type conversion
def column_name_finder(filepath) :
	"""This function will return a dictiory which will help us in finding matching function"""
	d = { }
	f = pd.ExcelFile (filepath)
	for sheet in f.sheet_names :
		df = f.parse (sheet)
		for column in df.columns :
			if ':' not in column :
				d[ str (column) ] = column
			else :
				d[ str (column[ :column.index (":") ]) ] = column
	return d


def range_creater(num) :
	"""Num should be less than 703"""
	a = num // 26
	b = num % 26
	if num <= 26 :
		return 'A' + ':' + chr (64 + num)
	elif a > 0 and b > 0 :
		return "A" + ":" + chr (64 + a) + chr (64 + b)
	elif a > 0 and b == 0 :
		return 'A' + ':' + chr (64 + a) + 'A'


def change_int_format(df , column_name) :
	list_of_column = {
		'Length of relationship_General' : 'Int64' , 'Net days_General' : 'Int64' , 'To net days_General' : 'Int64' ,
		'Area code_General' : 'Int64' , 'Amount_History' : 'Int64' ,
		'Post code_Relcon' : 'Int64' , 'Post code_Bank' : 'Int64' , 'Ord shares held(number)_History' : 'Int64' ,
		'Pref shares held (number)_History' : 'Int64' ,
		'Affiliate owns shares in subject_Relcon' : 'Int64' , 'This affiliate is a joint venture_Recon' : 'Int64' ,
		'Affiliate DUNS_Relcon' : 'Int64' , 'DUNS_Relcon' : 'Int64' ,
		'Ultimate parent DUNS_Relcon' : 'Int64' , 'Ordinary Shares (number)_History' : 'Int64' ,
		'Pref Shares (numbers)_History' : 'Int64' , 'Shares publicly listed_History' : 'Int64' ,
		'Ordinary shares (Amount)_History' : 'Int64' , 'Par value_History' : 'Int64' ,
		'International_General' : 'Int64' , 'Total employees_General' : 'Int64' , 'Local_General' : 'Int64' ,
		'Local purchase terms_General' : 'Int64' , 'Import terms_General' : 'Int64' ,
		'Local sales terms_General' : 'Int64' , 'Export sales terms_General' : 'Int64' ,
		'Months covered_General' : 'Int64'
		}
	
	formats = { }
	
	for key , val in list_of_column.items ( ) :
		formats[ column_name[ key ] ] = val
	
	ff = { i : formats[ i ] for i in df.columns.values.tolist ( ) if i in formats }
	
	try :
		for key , value in ff.items ( ) :
			df[ key ] = df[ key ].apply (lambda x : x.strip ( ).replace (' ' , '') if isinstance (x , str) else x)
		df = df.astype (ff)
	except Exception as e :
		print (ff , e)
	return df


def change_date_format(df , column_name) :
	date_formats = [ 'Shareholding as at_History' , 'Report date_Supplier' , 'Last AGM date_History' ,
	                 'Last financial end date_History' ]
	
	for i in range (len (date_formats)) :
		date_formats[ i ] = column_name[ date_formats[ i ] ]
	
	ff = [ i for i in df.columns.values.tolist ( ) if i in date_formats ]
	for i in ff :
		try :
			df[ i ] = pd.to_datetime (df[ i ] , errors = 'coerce')
			df[ i ] = df[ i ].dt.strftime ('%Y-%m-%d')
		except Exception as e :
			print (ff , e)
	return df


def change_string_format(df , column_name) :
	string_formats = [ 'Identity Number_Mgmtbac' ]
	string_formats[ 0 ] = column_name[ string_formats[ 0 ] ]
	ff = [ i for i in df.columns.values.tolist ( ) if i in string_formats ]
	for i in ff :
		try :
			df[ i ] = df[ i ].apply (lambda x : '{0:0>8}'.format (x))
		except Exception as e :
			print (ff , e)
	return df


def UpdateFieldIDDataframes(df , column_name) :
	try :
		sheetname = str (df[ "SheetName" ])
		if "Mgmt" in sheetname :
			df[ 'sid' ] = df.groupby ('duns_no').cumcount ( ) + 1
			df[ 'sid' ] = -df[ 'sid' ].astype (int)
		
		elif "Bank Loop_Non Loop" in sheetname :
			#  Loops
			df[ 'id' ] = df.assign (
				temp = ~df.duplicated (subset = [ 'duns_no' , column_name[ 'Indian Bank Name_Bank' ] ])).groupby (
				'duns_no')[
				'temp' ].cumsum ( )
			
			df[ 'id' ] = -df[ 'id' ].astype (int)
			# FIELDS Loops
			df[ 'Fieldid' ] = df.groupby ([ 'duns_no' , column_name[ 'Indian Bank Name_Bank' ] ]).cumcount ( ) + 1
			df[ 'Fieldid' ] = -df[ 'Fieldid' ].astype (int)
		
		else :
			df[ 'id' ] = df.groupby ('duns_no').cumcount ( ) + 1
			df[ 'id' ] = -df[ 'id' ].astype (int)
			df.applymap (lambda x : x.strip ( ) if type (x) == str else x)
			if 'SIC' in sheetname :
				df[ column_name[ 'SIC extension_General' ] ] = df[ column_name[ 'SIC extension_General' ] ].astype (
					str).apply (lambda x : x.zfill (4))
	
	except Exception as ex :
		print (ex)
	return df


def string_replace(df , column_name) :
	for i in [ [ column_name[ 'Identity Number_Mgmtbac' ] , [ '0000<NA>' , '' ] ] ] :
		try :
			df[ i[ 0 ] ] = df[ i[ 0 ] ].replace (i[ 1 ][ 0 ] , i[ 1 ][ 1 ])
		except :
			pass
	return df


def update_val_in_FinNonFin(filepath , logger) :
	try :
		filename = "FinNonFin2.csv"
	except :
		filename = "FinNonFin1.csv"
	
	dx = pd.read_csv (filename)
	
	f = pd.ExcelFile (filepath)
	for sheet in f.sheet_names :
		df = f.parse (sheet)
		column_name = [ ]
		for column in df.columns :
			column_name.append (column)
		column_name.append ("SheetName")
		
		for j in range (len (dx)) :
			if dx.loc[ j , 'Sheets' ] == str (sheet) :
				if str (dx.loc[ j , 'ColumnList' ]) != str (column_name) :
					logger.error (sheet)
					logger.error (dx.loc[ j , 'ColumnList' ])
					logger.error (column_name)
					dx.loc[ j , 'ColumnList' ] = column_name
					dx.loc[ j , 'CellRange' ] = range_creater (len (column_name) - 1)
					break
	dx.to_csv ("FinNonFin2.csv" , index = False)


def main_zip(path):
	print(path)
	
	
	
	parts = os.listdir((path))
	parts = [parts[i:i+5] for i in range(0,len(parts),5)]
	
	rank =1
	a = 1
	
	
	
	for i in parts:
		
		if 'non' in path :
			CI = f'{path}//CBIG_Insert_{rank}_ba95dc97215c4b2f8e375e69bb188240'
		else :
			CI = f'{path}//CBIG_Delete_{rank}_ba95dc97215c4b2f8e375e69bb188240'
		
		if not os.path.isdir(CI):
			os.mkdir(CI)
			for j in i:
				if a >5:
					a=1
				try:
					if j.endswith('csv'):
						name = j.replace ('ak' , str (a))
						os.replace(path+'//'+j,f'{CI}//{name}')
				except Exception as ex:
					print(ex)
				a = a+1
			
			try:
				if 'non' in CI:
					shutil.make_archive (f'CBIG_Insert_{rank}_ba95dc97215c4b2f8e375e69bb188240' , 'zip',CI )
				else :
					shutil.make_archive (f'CBIG_Delete_{rank}_ba95dc97215c4b2f8e375e69bb188240' , 'zip' ,CI)
						
				
			except Exception as ex :
				print(ex)
				
			try:
				shutil.rmtree(CI)
			except Exception as e:
				print(e)
			
			rank = rank+1
			
	# for i in os.listdir(path):
	# 	if i.endswith('csv'):
	# 		os.remove(path+'\\'+i)

def validate_the_data(logger) :
	dirname = os.path.dirname (__file__)
	base = dirname.rsplit ('//' , 1)[ 0 ]
	dir = os.path.join (base , 'OutputFiles\\ReportToDews')
	multicsv_delete = multi['delete']
	multicsv_nondelete = multi['nonDelete']
	
	if not os.path.exists (multi['delete']) :
		os.mkdir (multi['delete'])
	
	if not os.path.exists (multi['nonDelete']) :
		os.mkdir (multi['nonDelete'])
	
	Inputdirectory = InPutFilesReportToDews[ "InPutFilesReportToDews" ]
	for file in os.listdir (Inputdirectory) :
		input_file = file.split ('.')[ 0 ]
		file_name = Path (os.path.join (Inputdirectory , file))
		# updating the FinNonFin.csv for dynamicically validating
		if not file.endswith (".xlsx") :
			continue
		update_val_in_FinNonFin (file_name , logger)
		column_name = column_name_finder (file_name)
		
		records = pd.read_csv ('FinNonFin2.csv').to_dict (orient = 'records')
		print (records)
		status_list = [ ]
		Outputdirectory = OutPutFilesReportToDews[ "OutPutFilesReportToDews" ]
		if not os.path.exists (Outputdirectory) :
			os.makedirs (Outputdirectory)
		for i in os.listdir (Outputdirectory) :
			if '.csv' in i :
				os.remove (Outputdirectory + '\\' + i)
		
		for i in os.listdir (multicsv_delete) :
			if '.csv' in i :
				os.remove (multicsv_delete + '\\' + i)
		for i in os.listdir (multicsv_nondelete) :
			if '.csv' in i :
				os.remove (multicsv_nondelete + '\\' + i)
		
		for record in records :
			sheet_name = record[ 'SheetName' ]
			print ('Sheet Name: ' , sheet_name)
			today = datetime.today ( ).strftime ('%Y-%m-%d')
			log_time = datetime.now ( ).strftime ('%Y-%m-%d %H:%M:%S')
			
			key = record[ 'SheetName' ]
			token = record['token']
			
			cell_range = record[ 'CellRange' ]
			deletion = record[ 'Deletion' ]
			if cbignonfin_env[ "Env" ] == "prod" :
				insert_token_id = record[ 'prod_insert' ]
				del_token_id = record[ 'prod_del' ]
				insert_key = record[ 'sand_insert_key' ]
				delete_key = record[ 'sand_delete_key' ]
			if cbignonfin_env[ "Env" ] == "sand" :
				insert_token_id = record[ 'sand_insert' ]
				del_token_id = record[ 'sand_del' ]
				insert_key = record[ 'sand_insert_key' ]
				delete_key = record[ 'sand_delete_key' ]
			
			df = pd.read_excel (file_name , sheet_name = sheet_name , usecols = cell_range)
			
			if not df.empty :
				df[ 'SheetName' ] = sheet_name
				df.rename (columns = { 'DUNS_General:aa' : 'duns_no' } , inplace = True)
				df = UpdateFieldIDDataframes (df , column_name)
				
				df = change_int_format (df , column_name)
				
				df = change_date_format (df , column_name)
				df = change_string_format (df , column_name)
				df = string_replace (df , column_name)
			
			if record[ 'type1' ] == 'single' :
				df.to_csv (OutPutFilesReportToDews[ 'OutPutFilesReportToDews' ] + sheet_name + "_All_" + today + "_" + insert_token_id + ".csv" ,index = False)
				if deletion == 'Y' :
					df.dropna (how = 'all' , inplace = True)
					df[ 'duns_no' ][ :1 ].to_csv (
						Outputdirectory + sheet_name + "_Delete_" + today + "_" + del_token_id + ".csv")
			elif record[ 'type1' ] == 'multi' :
				try :
					df.to_csv (multicsv_nondelete + '\\' + sheet_name + '_ALL_' + "_" +'ak'+ "_"+insert_key + ".csv" , index = False)
					
				except Exception as e:
					print (e)
				if deletion == 'Y' :
					df.dropna (how = 'all' , inplace = True)
				try :
					df[ 'duns_no' ][ :1 ].to_csv (multicsv_delete + '\\' + sheet_name + '_All_' + "_"+'ak'+"_" + delete_key + ".csv")
				except :
					pass
			elif record[ 'type1' ] == 'single,multi' :
				df.to_csv (OutPutFilesReportToDews[
					           'OutPutFilesReportToDews' ] + sheet_name + "_All_" + today + "_" + insert_token_id + ".csv" ,
				           index = False)
				if deletion == 'Y' :
					df.dropna (how = 'all' , inplace = True)
					df[ 'duns_no' ][ :1 ].to_csv (
						Outputdirectory + sheet_name + "_Delete_" + today + "_" + del_token_id + ".csv")
				
				df.to_csv (multicsv_nondelete + '\\' + sheet_name + '_ALL_' + insert_key + ".csv" , index = False)
				if deletion == 'Y' :
					df.dropna (how = 'all' , inplace = True)
					df[ 'duns_no' ][ :1 ].to_csv (multicsv_delete + '\\' + sheet_name + '_ALL_' + delete_key + ".csv")
					
			
			
			else :
				status = "File Processing Failed"
				
				status_list.append ([ "Failed" , file_name , str (key) , status , str (today) + "_" + str (log_time) ,
				                      "Worksheet is empty" ])
	
	parts = os.listdir(multicsv_delete)
			
	main_zip (multicsv_delete)
	main_zip (multicsv_nondelete)
