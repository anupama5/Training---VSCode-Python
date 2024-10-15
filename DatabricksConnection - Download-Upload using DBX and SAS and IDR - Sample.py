# -*- coding: utf-8 -*-
"""
Created on Tue May 1 12:05:32 2024

@author: L645141
"""


#Program:
#   This program does two funtions based on the input parameters
#   1. Download data from IDR and copy to local machine
#   2. Download data from Databricks(DBX) and copy it to SAS location
#Input
#   1. Input.CSV >  This file holds the details of the tables to be downloaded
#output:
#   1. Copy's IDR table to local machine as CSV
#   2. Copy's DBX tables to local machine and then copy it to SAS location
 


#import packages required for the program
from databricks import sql  # Required for Databricks call
import cx_Oracle as oracle  # Required for Oracle call 
import pandas as pd  # Required for data frame manipulation
import sas7bdat  # Required for SAS call
from datetime import datetime
import os
import sys
import paramiko


ARYSIZE=5000
ProgramName='DatabricksToSAS'
#input and output location - excel with list of tables
outputLocation='C:\\Azure\\Batch\\Output\\SharedEnrichment\\'
remoteLocation='/gpfs/FS2/sasdata/adhoc/kpinsight/shared/data/DBX/AnuN/'
inputLocation = outputLocation
inputFilename='Input1.csv'


now = datetime.now()
currentTime=  now.strftime("%Y-%m-%d-%H:%M:%S:%f")
Extensionfull=  now.strftime("%Y%m%d%H%M%S%f")

FileWrite = open(outputLocation+ProgramName+Extensionfull+'_log.txt', 'w')
message ='\nCurrent timestamp: {}'.format(currentTime)
print(message)
FileWrite.write(message)


#read input csv file from the inputLocation
inputDF = pd.read_csv(inputLocation+inputFilename)  
message ='\nNumber of tables in the list: {}'.format(len(inputDF))
print(message)
FileWrite.write(message)

# #read input XLSX file from the inputLocation
# print(inputLocation+inputFilename)
# inputDF = pd.read_excel(inputLocation+inputFilename,sheet_name=0,keep_default_na=False)
# message ='\nNumber of tables in the list: {}\n'.format(len(inputDF))
# print(message)
# FileWrite.write(message)

inputDF=inputDF.loc[inputDF['Download']=='Y']
message ='\nNumber of tables in the list to download: {}\n'.format(len(inputDF))
print(message)
#print(inputDF)
FileWrite.write(message)

# Funtion to create Oracle Query
def CreateOracleQuery(tableName, regionCode,year,month):

    QRY="""          
     SELECT  *
     FROM {0}
     --where RGN_CD='08' and  UTLZTN_YR_NB ={2} and UTLZTN_MM_NB = {3}   
  """.format(tableName,regionCode,year,month)
    print(QRY)
    return(QRY)

# Funtion to connect to Oracle DB,execute the Query and retrieve results in a dataframe
def ConnectToOracle(QRY):

    host   = 'scan-nzx412.nndc.kp.org'
    port   = 1521
    svc_nm = 'csidrp1r'

    user   = 'XXX'
    pwd    = 'XXX'

    RGN='Prod'
    database='MetricsMart'
        # Create a Data Source Name(DSN) for this connection
    dsn_tns = oracle.makedsn(host, port, service_name = svc_nm)
        # Connect to the database
    con = oracle.connect(user, pwd, dsn_tns)
    print('Connected to',database,'for',RGN, 'region')
    cur = con.cursor()
    cur.arraysize=ARYSIZE
    cur.execute(QRY) 
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows,columns=names)
#    print(len(df))
    return(df)

# Funtion to connect to Databricks DB,execute the Query and retrieve results in a dataframe

def ConnectToDBX(QRY,database_enviroment,outputLocation,tableName,FileWrite):
    ret=0
    message ='\ndatabase_enviroment : {}'.format(database_enviroment)
    print(message)
    FileWrite.write(message)

    if database_enviroment=='PSUP':
        # Connection parameters for psup
        host="adb-6515962755123314.14.azuredatabricks.net"
        http_path ="/sql/1.0/warehouses/5dae5fe909f3c246"
        access_token='ENR_X-Small_PYTHON_CONNECT'
    else:
        # Connection parameters for dev
        host="adb-3025435396364713.13.azuredatabricks.net"
        http_path ="/sql/1.0/warehouses/4f36f86331f7b3c7"
        access_token='ENR_X-Small_PYTHON_CONNECT'
        
    print(QRY)
    # cnxn = sql.connect(
    #         server_hostname=host,
    #         http_path=http_path,
    #         access_token=access_token)
#    cur
    cnxn = sql.connect(
            server_hostname=host,
            http_path=http_path,
            auth_type="databricks-oauth")
    
    print('Connected to DBX',database_enviroment)
    df = pd.read_sql_query(QRY, con = cnxn)
    message ='\nLength of DBX table {}: {}'.format(tableName,len(df))
    print(message)
    FileWrite.write(message)
    df.to_csv(outputLocation+tableName+'.csv',index=False)
        
#    print(len(df))
    message ='\nOutfile created'
    print(message)
    FileWrite.write(message)
    if len(df)>0:
        ret=0
    else:
        message ='\nEmpty file not copied'
        print(message)
        FileWrite.write(message)
        ret=2
    return(ret)

# Funtion to create Databricks Query
def CreateDBXQuery(tableName, regionCode,year,month):

    QRY="""          
SELECT  *
    FROM {0}
 --   where RGN_CD='{1}' and UTLZTN_YR_NB ={2} and UTLZTN_MM_NB = {3}
 """ .format(tableName,regionCode,year,month)
    return(QRY)

# Funtion to connect to SAS and write the CSV file
def SASConnWrite(outputLocation,remoteLocation,tableName):
    ret=0
    message='\nSuccessfully copied file to SAS server'
    start = datetime.now()
    username = 'XXX'
    password = 'XXX'  

    hostname='nzxpap1148.nndc.kp.org'
    remotefilepath= remoteLocation+tableName+'.csv'
    localfilepath=outputLocation+tableName+'.csv'
    
    message ='\nSAS Server name : {}'.format(hostname)
    print(message)
    FileWrite.write(message) 
    message ='\nSAS folder      : {}'.format(remotefilepath)
    print(message)
    FileWrite.write(message)   
    message ='\nLocal folder    : {}'.format(localfilepath)
    print(message)
    FileWrite.write(message)

    ssh_client=paramiko.SSHClient()

    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh_client.connect(hostname=hostname,username=username,password=password)
    except:
        message="ssh_client.connect error"
        ret=1  

    try:
        ftp_client=ssh_client.open_sftp()
    except:
        message="ssh_client.open_sftp() error"
        ret=1  

    try:
        ftp_client.put(localfilepath,remotefilepath)
        message="successfully uploaded file "   
        ret=0
    except:
        message="ftp_client.put error"
        ret=1
   
    ftp_client.close()

    return(ret,message)

# Funtion to connect to IDR and download the CSV file
def IDRtoLocal(outputLocation,database_enviroment,SchemaName,tableName,FileWrite):
    ret=0
    SchemaTable=SchemaName+'.'+tableName  
    IDR_Query=CreateOracleQuery(SchemaTable,0,0,0)
#   print(IDR_Query)
    IDROutputDF=ConnectToOracle(IDR_Query)
    #ret=ConnectToOracle(IDR_Query,outputLocation,tableName,FileWrite)
    if len(IDROutputDF)<=0:
        message ='\nIDR table empty'
        print(message)
        FileWrite.write(message)
        return(2)
    else:
        message ='\nLength of IDR table {}: {}'.format(tableName,len(IDROutputDF))
        print(message)
        FileWrite.write(message)

    message ='\nWrite file to output location {}'.format(outputLocation)
    print(message)
    FileWrite.write(message)   
    IDROutputDF.to_csv(outputLocation+tableName+'.csv',index=False)

    return(ret)

#funtion to check the request and drive to IDR or DBX
def DataDownload(outputLocation,remoteLocation,database,database_enviroment,SchemaName,tableName,FileWrite):
    message ='\n-------------------------------------------------------------------'
    print(message)
    FileWrite.write(message)

    now = datetime.now()
    currentTime=  now.strftime("%Y-%m-%d-%H:%M:%S:%f")
    message ='\nProcess Start timestamp : {}'.format(currentTime)
    print(message)
    FileWrite.write(message)
    
    if database=='IDR':
        message ='\nGet data from IDR table {}.{}'.format(SchemaName,tableName)
        print(message)
        FileWrite.write(message)
        ret=IDRtoLocal(outputLocation,database_enviroment,SchemaName,tableName,FileWrite)
        
    else:
        message ='\nGet data from DBX table {}.{}'.format(SchemaName,tableName)
        print(message)
        FileWrite.write(message)
        ret=DBXtoSas(outputLocation,remoteLocation,database_enviroment,SchemaName,tableName,FileWrite)
    
    currentTime=  now.strftime("%Y-%m-%d-%H:%M:%S:%f")
    message ='\nProcess End timestamp   : {}'.format(currentTime)
    print(message)
    FileWrite.write(message)
    return(ret)

# Funtion to connect to DBX, download the CSV file and copy to remote SAS location
def DBXtoSas(outputLocation,remoteLocation,database_enviroment,SchemaName,tableName,FileWrite):

    SchemaTable=SchemaName+'.'+tableName  
    DBX_Query=CreateDBXQuery(SchemaTable,0,0,0)
#    print(DBX_Query)

    ret=ConnectToDBX(DBX_Query,database_enviroment,outputLocation,tableName,FileWrite)

    if ret>0:
        message ='\nDBX Error: {}'.format(ret)
        print(message)
        FileWrite.write(message)
        return(1) 

    now = datetime.now()
    currentTime=  now.strftime("%Y-%m-%d-%H:%M:%S:%f")
    message ='\nDBX Completed timestamp: {}'.format(currentTime)
    print(message)
    FileWrite.write(message)

    if ret==0:
        ret,messageSAS=SASConnWrite(outputLocation,remoteLocation,tableName)
        message ='\nSas return message: {}'.format(messageSAS)
        print(message)
        FileWrite.write(message)
        
    now = datetime.now()
    currentTime=  now.strftime("%Y-%m-%d-%H:%M:%S:%f")
    message ='\nSAS Completed timestamp  : {}'.format(currentTime)
    print(message)
    FileWrite.write(message)
    return(ret)

#print(inputDF.info())

#Call program for each table in the list
if len(inputDF)>0:
    inputDF['returncode'] = inputDF.apply(lambda row: DataDownload(outputLocation,remoteLocation,row['Database'],row['Environment'],row['SchemaName'],row['TableName'],FileWrite), axis=1)
else:
    message ='\nNo Table to process'
    print(message)
    FileWrite.write(message)
message ='\n-------------------------------------------------------------------'
print(message)
FileWrite.write(message)
    
##############################################################
#Close the program log
#############################################################
message ='\nProgram successfully completed'
print(message)
FileWrite.write(message)

now = datetime.now()
currentTime=  now.strftime("%Y-%m-%d-%H:%M:%S:%f")
message ='\nCurrent timestamp: {}'.format(currentTime)
print(message)
FileWrite.write(message)

FileWrite.close()
