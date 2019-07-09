#!/usr/bin/env python

#+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+
#|T|R|A|N|S| |C|H|E|C|K|E|R| |2|
#+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+

# - Imports 
import sys
import json
import os
import string
from random import *
import re
from datetime import datetime
import sqlite3
from sqlite3 import Error
import numpy as np
import pg8000
import logging

#import pdb
#import ast

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

# - Static definitions
FILE_DOES_NOT_EXIST_TRUE = "Source file doesn't exist"

# - SESSION LOG - Valid Keys
DEFAULT_KEYS_SESSIONLOG = ['LogData','SessionId']

# - SESSION LOG - Valid parameters
DEFAULT_PARAMETER_LIST_SESSIONLOG_LOGDATA = ['action','address','clientAddressPath','protocol','sid','time','who']

# - AUDIT LOG - Valid keys
DEFAULT_KEYS_AUDITLOG = ['InitiatingUser','RealUser','LoggingTime','SessionId','TransactionId','TransactionType','Status','RecordVersion','SupplementaryData']

# - AUDIT LOG - Valid parameters
DEFAULT_PARAMETER_LIST_AUDITLOG_SUPPLEMENTARYDATA = ['AccountHolderID','ListOfAccountFRIs']

# - PROCESSING VALUES
#PROCESSING_KEYS_FOR_AUDITLOG = ['LoggingTime','SessionId','TransactionId','TransactionType','Status']
PROCESSING_KEYS_FOR_AUDITLOG = ['LoggingTime','SessionId','TransactionType','Status']
PROCESSING_KEYS_FOR_SESSIONLOG1 = ['LogData']
PROCESSING_KEYS_FOR_SESSIONLOG2 = ['protocol','sid','time']

# - Separator
SEPARATOR = ';'
ITEM_SEPARATOR = '.'

# - Database Staging 1
STAGE1 = """CREATE TABLE STAGE1 (
    DATE     DATE NOT NULL,
    SID      VARCHAR NOT NULL,
    PROTOCOL TEXT NOT NULL
);"""
#SID      VARCHAR UNIQUE NOT NULL,

# - STAGE 1 QUERIES
STAGE1_QUERY1 = 'INSERT INTO STAGE1 ( DATE,SID,PROTOCOL ) VALUES ('

# - Database Staging 2
STAGE2 = """CREATE TABLE STAGE2 (
    DATE            DATE NOT NULL,
    SID             TEXT NOT NULL,
    TRANSACTIONTYPE TEXT NOT NULL,
    STATUS          TEXT NOT NULL
);"""

# - STAGE 2 QUERIES
STAGE2_QUERY1 = 'INSERT INTO STAGE2 ( DATE,SID,TRANSACTIONTYPE,STATUS ) VALUES ('

# - Database Staging 3
STAGE3 = """CREATE TABLE STAGE3 (
    DATE     DATE    NOT NULL,
    SID      VARCHAR NOT NULL,
    PROTOCOL TEXT    NOT NULL,
    COUNT    INTEGER  NOT NULL
);"""

# - STAGE 3 QUERIES
STAGE3_QUERY1 = 'INSERT INTO STAGE3 SELECT DATE,SID,PROTOCOL,COUNT(*) FROM STAGE1 GROUP BY DATE,SID,PROTOCOL;'

# - Database Staging 4
STAGE4 = """CREATE TABLE STAGE4 (
    DATE            DATE    NOT NULL,
    SID             TEXT    NOT NULL,
    TRANSACTIONTYPE TEXT    NOT NULL,
    STATUS          TEXT    NOT NULL,
    COUNT           INTEGER NOT NULL,
    PROTOCOL        TEXT
);"""

# - STAGE 4 QUERIES
STAGE4_QUERY1 = 'INSERT INTO STAGE4 SELECT DATE,SID,TRANSACTIONTYPE,STATUS,COUNT(*),\'\' FROM STAGE2 GROUP BY SID,TRANSACTIONTYPE,STATUS,DATE;'

# - Database final stage
FINAL_STAGE1 = """CREATE TABLE FINALSTAGE1 (
    DATE             DATE,
    SID              TEXT,
    PROTOCOL         TEXT,
    TRANSACTIONTYPE  TEXT,
    STATUS           TEXT,
    TRANSACTIONCOUNT INTEGER,
    SESSIONEVENTS    INTEGER,
    AUDITFILE        TEXT,
    SESSIONFILE      TEXT
);"""

FINAL_STAGE2 = """CREATE TABLE FINALSTAGE2 (
    DATE             DATE,
    SID              TEXT,
    PROTOCOL         TEXT,
    TRANSACTIONTYPE  TEXT,
    STATUS           TEXT,
    TRANSACTIONCOUNT INTEGER,
    SESSIONEVENTS    INTEGER,
    AUDITFILE        TEXT,
    SESSIONFILE      TEXT
);"""

# - FINAL STAGE QUERIES
FINAL_STAGE_QUERY1 = 'SELECT * FROM STAGE4;'
FINAL_STAGE_QUERY2 = 'SELECT COUNT(*) FROM STAGE3 WHERE SID = \''
FINAL_STAGE_QUERY3 = 'SELECT * FROM STAGE3 WHERE SID = \''
FINAL_STAGE_QUERY4 = 'INSERT INTO FINALSTAGE1 ( DATE,SID,PROTOCOL,TRANSACTIONTYPE,STATUS,TRANSACTIONCOUNT,SESSIONEVENTS,AUDITFILE,SESSIONFILE ) VALUES ('
FINAL_STAGE_QUERY5 = 'INSERT INTO FINALSTAGE2 ( DATE,SID,PROTOCOL,TRANSACTIONTYPE,STATUS,TRANSACTIONCOUNT,SESSIONEVENTS,AUDITFILE,SESSIONFILE ) VALUES ('

# - STAGE file extension
STAGE1_EXTENSION = 'STAGE1.sqlite'
STAGE2_EXTENSION = 'STAGE2.sqlite'

# - CONSOLIDATION STAGE
CONSOLIDATION_TABLE1 = """CREATE TABLE "cons_table_session_log" (
  DATE  DATE,
  SID   TEXT,
  PROTOCOL TEXT,
  COUNT INTEGER
);"""

CONSOLIDATION_TABLE2 = """CREATE TABLE "cons_table_audit_log" (
  DATE DATE,
  SID TEXT,
  TRANSACTIONTYPE TEXT,
  STATUS TEXT,
  COUNT BIGINT,
  PROTOCOL TEXT
);"""

CONSOLIDATION_TABLE1_INDEX = 'CREATE INDEX "sid_index" ON "cons_table_session_log" USING btree (SID);'
CONSOLIDATION_TABLE2_INDEX1 = 'CREATE INDEX "sid1_index" ON "cons_table_audit_log" USING btree (SID);'
CONSOLIDATION_TABLE2_INDEX2 = 'CREATE INDEX "status_index" ON "cons_table_audit_log" USING btree (STATUS);'

# - Data consolidation in RDS
SELECT_ALL_SESSION_LOG = 'SELECT * FROM STAGE3;'
SELECT_ALL_AUDIT_LOG = 'SELECT * FROM STAGE4;'

# - Insert session data
INSERT_SESSION_DATA = 'INSERT INTO "RDS".cons_table_session_log ( date, sid, protocol, count ) VALUES ('
SELECT_ALREADY_INSERTED_SESSION_DATA = 'SELECT COUNT(*) FROM cons_table_session_log WHERE SID='
UPDATE_ALREADY_INSERTED_SESSION_DATA = 'UPDATE cons_table_session_log SET COUNT = COUNT +'

# - Insert audit data
INSERT_AUDIT_DATA = 'INSERT INTO "RDS".cons_table_audit_log ( date,sid,transactiontype,status,count ) VALUES ('
SELECT_ALREADY_INSERTED_AUDIT_DATA = 'SELECT COUNT(*) FROM "RDS".cons_table_audit_log WHERE SID='
UPDATE_ALREADY_INSERTED_AUDIT_DATA = 'UPDATE "RDS".cons_table_audit_log SET COUNT = COUNT + '

# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+
# |C|L|A|S|S| |D|E|F|I|N|I|T|I|O|N|
# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+

class OsAgent:
    parametersList = []
    parametersSize = 0
    myFile = ""
    extraFile = ""
    function = ""
    #myKey = []
    #myParametersPerKey = []
    myFileExist = 0

    def argumentList(self,list):
        self.parametersList = list

    def countMyParameters(self):
        self.parametersSize = len(self.parametersList)

    def parsingParameters(self):
        self.myFile = self.parametersList[1]
        self.function = self.parametersList[2]

    def checkSourceFile(self,myInt):
        self.myFileExist = myInt

    @staticmethod
    def clearScreen():
        os.system('clear')

    @staticmethod
    def elegantExit():
        sys.exit()

    @staticmethod
    def getRandomName(preName,extension):
        allchar = string.ascii_letters + string.digits
        min_char = 8
        max_char = 12
        randomExtension = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
        outputFileName = preName + '.' + randomExtension + '.' + extension
        return outputFileName

    @staticmethod
    def cleanAtoms(myValue):
        if type(myValue) == unicode:
            renValue = myValue.encode('ascii')
        else:
            renValue = str(myValue)
        return renValue

    @staticmethod
    def uniqueArray(myArray):
        outputObject = []
        myTempNPArray = np.array(myArray)
        finalUniqueArray = np.unique(myTempNPArray,axis=0)
        for i in finalUniqueArray:
            x,y,z = i
            myTempTuple = (x,y,z)
            outputObject.append(myTempTuple)
        return outputObject

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class JSONFileBox:
    currentQueryFileName = ""
    currentFile = None
    myJSONFileList = []
    myJSONObjects = []
    sessionCollectionArray = []
    sessionUniqueArray = []
    sessionQueryArray = []
    auditCollectionArray = []
    auditQueryArray = []

    # - CountArray
    countArray = []
    consolidatedQueries = []
    
    def dumpMyJsonToFile(self,myJSON):
        with open(myJSON,'r') as jsonRaw:
            for indice,jsonData in enumerate(jsonRaw):
                self.currentQueryFileName = OsAgent.getRandomName('JSON','json')
                with open(self.currentQueryFileName,'w') as write_file:
                    write_file.write(jsonData)
                    self.myJSONFileList.append(self.currentQueryFileName)

    def loadJsons(self):
        for myfile in self.myJSONFileList:
            myTempString = open(myfile,'r').read()
            os.remove(myfile)
            # - Check Json Detail
            tempJson = json.loads(myTempString)
            temporalTuple = ( myfile, tempJson )
            #self.myJSONObjects.append(tempJson)
            self.myJSONObjects.append(temporalTuple)

    def auditCollection(self):
        for myFile,jsonDictionary in self.myJSONObjects:
            # - Get LoggingTime,SessionId,TransactionType,Status
            if jsonDictionary['TransactionType'] != 'LOG_FILE_CREATION':
                # - Is not a log creation entry
                for auditLogKey in PROCESSING_KEYS_FOR_AUDITLOG:
                    # - Process the valid Keys for Audit Log
                    MyValue = jsonDictionary[auditLogKey]
                    if auditLogKey == 'LoggingTime':
                        MyDateTime = datetime.strptime(MyValue,"%Y-%m-%d %H:%M:%S.%f").date()
                        MyDateString = MyDateTime.strftime('%Y%m%d')
                    elif auditLogKey == 'SessionId':
                        MySessionID = OsAgent.cleanAtoms(MyValue)
                    elif auditLogKey == 'TransactionType':
                        MyTransactionType = OsAgent.cleanAtoms(MyValue)
                    elif auditLogKey == 'Status':
                        MyStatus = OsAgent.cleanAtoms(MyValue)
                myTemporalTuple = (MyDateString,MySessionID,MyTransactionType,MyStatus)
                self.auditCollectionArray.append(myTemporalTuple)

    def sessionCollection(self):
        for myFile,jsonDictionary in self.myJSONObjects:
            # - Collect Session Log detail
            for sessionLogKey in PROCESSING_KEYS_FOR_SESSIONLOG1:
                # - Process LogData
                if jsonDictionary[sessionLogKey]['action'] != 'SESSION_INVALID':
                    for sessionValues in PROCESSING_KEYS_FOR_SESSIONLOG2:
                        # - Process Protocol and SID
                        MyValue = jsonDictionary[sessionLogKey][sessionValues]
                        if sessionValues == 'protocol':
                            MyProtocol = OsAgent.cleanAtoms(MyValue)
                        elif sessionValues == 'sid':
                            MySID = OsAgent.cleanAtoms(MyValue)
                        elif sessionValues == 'time':
                            MyDateTime = datetime.strptime(MyValue[0:23],"%Y-%m-%dT%H:%M:%S.%f").date()
                            MyDateString = MyDateTime.strftime('%Y%m%d')
        
            myTemporalTuple = (MyDateString,MySID,MyProtocol)
            self.sessionCollectionArray.append(myTemporalTuple)

        # - Get Unique Array
        #self.sessionUniqueArray = OsAgent.uniqueArray(self.sessionCollectionArray)

    def sessionQueryBuilder(self):
        #for myTuple in self.sessionUniqueArray:
        for myTuple in self.sessionCollectionArray:
            MyDateString,mySID,MyProtocol = myTuple
            myTempQuery = '\'%s\',\'%s\',\'%s\');' % ( MyDateString,mySID,MyProtocol )
            myFinalQuery = STAGE1_QUERY1 + myTempQuery
            self.sessionQueryArray.append(myFinalQuery)

    def auditQueryBuilder(self):
        for myTuple in self.auditCollectionArray:
            MyDateTime,MySessionID,MyTransactionType,MyStatus = myTuple
            myTempQuery = '\'%s\',\'%s\',\'%s\',\'%s\');' % ( MyDateTime,MySessionID,MyTransactionType,MyStatus )
            myFinalQuery = STAGE2_QUERY1 + myTempQuery
            self.auditQueryArray.append(myFinalQuery)

    def buildCountQueries(self,myAuditArray,staticQuery1,staticQuery2):
        for myRow in myAuditArray:
            myDate,mySID,myTransType,myStatus,myCount,myProtocol = myRow
            myString1 = staticQuery1 + mySID + '\';'
            myString2 = staticQuery2 + mySID + '\';'
            myTuple = ( myDate,mySID,myTransType,myStatus,myCount,myProtocol,myString1,myString2 )
            self.countArray.append(myTuple)

    def finalConsolidation(self,myCountArray,myDBObject,auditFileName,sessionFileName,foundRegisterQuery,notFoundRegisterQuery):
        for myQuery in myCountArray:
            myDate,mySID,myTransType,myStatus,myCount,myProtocol,query1,query2 = myQuery
            myResult = myDBObject.queryDB(query1)
            if myResult[0][0] == 1:
                myResult2 = myDBObject.queryDB(query2)
                myDate2,mySID2,myProtocol2,mySessionEvents = myResult2[0]
                myTempTuple = ( myDate,mySID,myProtocol2,myTransType,myStatus,myCount,mySessionEvents,auditFileName,sessionFileName )
                myTempQuery = foundRegisterQuery + '\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');' % ( myDate, mySID, myProtocol2, myTransType, myStatus, myCount, mySessionEvents, auditFileName, sessionFileName )
            else:
                mySessionEvents=''
                myTempTuple = ( myDate,mySID,myProtocol,myTransType,myStatus,myCount,mySessionEvents,auditFileName,sessionFileName )
                myTempQuery = notFoundRegisterQuery + '\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');' % ( myDate, mySID, myProtocol, myTransType, myStatus, myCount, mySessionEvents, auditFileName, sessionFileName )

            self.consolidatedQueries.append(myTempQuery)


    @staticmethod
    def printInvalidKeys(listName,key,subkey):
        INTERNAL_LONG_DETAIL_STRING = 'Missing Key or SubKey to be reported: ' 
        INTERNAL_SEPARATOR = ' : '
        internalString = INTERNAL_LONG_DETAIL_STRING + listName + INTERNAL_SEPARATOR + key + INTERNAL_SEPARATOR + subkey
        print internalString

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class PersistenceBox:
    
    myCurrentDatabase = ""
    extraFile = ""

    def createDB(self,myFile,extension):
        self.myCurrentDatabase = OsAgent.getRandomName(myFile,extension)

        try:
            # - Create database
            MyConnection = sqlite3.connect(self.myCurrentDatabase)
            # - Setup my cursor
            MyCursor = MyConnection.cursor()
        except Error as e:
            print(e)
        finally:
            MyConnection.close()

    def createStage(self,stageTable):
        try:
            MyConnection = sqlite3.connect(self.myCurrentDatabase)
            MyCursor = MyConnection.cursor()
            # - Commit STAGE1 Object
            MyCursor.execute(stageTable)
            MyConnection.commit()
        except Error as e:
            print(e)
        finally:
            MyConnection.close()

    def queryDB(self,query):
        try:
            MyConnection = sqlite3.connect(self.myCurrentDatabase)
            MyCursor = MyConnection.cursor()
            MyCursor.execute(query)
            MyConnection.commit()
            MyResult = MyCursor.fetchall()
            return MyResult
        except Error as e:
            print(e)
        finally:
            MyConnection.close()

    def insertBulkDB(self,queryArray):
        try:
            MyConnection = sqlite3.connect(self.myCurrentDatabase)
            MyCursor = MyConnection.cursor()
            for query in queryArray:
                MyCursor.execute(query)
                #print MyCursor.lastrowid
            MyConnection.commit()
        except Error as e:
            print(e)
        finally:
            MyConnection.close()

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class DBbox:

    user = "RDS"
    passwd = "ABc123456"
    adminPasswd = "ABc123456"
    database = "RDS"
    port = "5432"
    host = "redhat6"
    conexion = None
    internalCursor = None
    cursorLenght = 0
    resultQuery = None
    resultQueryClean = []

    def __init__(self,user,passwd,database,port):
        self.user = user
        self.passwd = passwd
        self.database = database
        self.port = port

    def openConnection(self, cls):
        try:
            self.conexion = pg8000.connect( user = self.user, host = self.host, database = self.database, password = self.passwd, port = self.port )
        except Exception as err:
            #print(err)
            cls.exception("Exception occurred connecting to the database")

    def closeConnection(self, cls):
        try:
            self.conexion.close()
        except Exception as err:
            #print(err)
            cls.exception("Exception occurred during closing session with the database")

    def queryRDS(self,query):
        try:
            self.internalCursor = self.conexion.cursor()
            self.internalCursor.execute(query)
            self.conexion.commit()
            self.resultQuery = self.internalCursor.fetchall()

            # - Codigo Limpia Shit
            for i in self.resultQuery:
                if type(i[0]) == unicode:
                    self.resultQueryClean.append( i[0].encode('ascii') )

            self.cursorLenght = len(self.resultQuery)
        except Exception as err:
            print(err)

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class postgresQueryBox:
    
    finalSessionQueryList = []
    finalAuditQueryList = []

    def sessionLogQueryCreation(self,myQueryResult,myStaticQuery,myStaticSearchQuery,myStaticUpdateQuery):
        for tempTuple in myQueryResult:
            #('20190501', '1514526567935', 'http-ussd', 1)
            myDate,mySID,myProtocol,myCount = tempTuple
            myTempQuery1 = myStaticSearchQuery + '\'%s\';' % (mySID)
            myTempQuery2 = myStaticQuery + ' \'%s\',\'%s\',\'%s\',%s );' % ( myDate,mySID,myProtocol,myCount )
            myTempQuery3 = myStaticUpdateQuery + '%s WHERE SID = \'%s\';' % ( myCount,mySID )
            myQueryTuple = (myTempQuery1,myTempQuery2,myTempQuery3)
            self.finalSessionQueryList.append(myQueryTuple)

    def auditLogQueryCreation(self,myQueryResult,myStaticQuery,myStaticSearchQuery,myStaticUpdateQuery):
        for tempTuple in myQueryResult:
            #(20190501, u'1514526562079', u'CancelPreApproval', u'SUCCESS', 1, u'')
            myDate,mySID,myTransactionTime,myStatus,myCount,myProtocol = tempTuple
            myTempQuery1 = myStaticSearchQuery + '\'%s\' AND TRANSACTIONTYPE=\'%s\' AND STATUS=\'%s\';' % ( mySID,myTransactionTime,myStatus )
            myTempQuery2 = myStaticQuery + ' \'%s\',\'%s\',\'%s\',\'%s\',%s );' % ( myDate,mySID,myTransactionTime,myStatus,myCount )
            myTempQuery3 = myStaticUpdateQuery + '%s WHERE SID = \'%s\' AND TRANSACTIONTYPE=\'%s\' AND STATUS=\'%s\';' % ( myCount,mySID,myTransactionTime,myStatus )
            myQueryTuple = (myTempQuery1,myTempQuery2,myTempQuery3)
            self.finalAuditQueryList.append(myQueryTuple)

# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+
# |D|E|P|L|O|Y|M|E|N|T| |L|O|G|I|C|
# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+

# - Logger
botLogger = logging.getLogger(__name__)
f_handler = logging.FileHandler('session_consolidation.log')
f_handler.setLevel(logging.ERROR)
f_format = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
f_handler.setFormatter(f_format)
botLogger.addHandler(f_handler)

# - Set Trace
#pdb.set_trace()

# - Objects
os1 = OsAgent()
jsonfile1 = JSONFileBox()
persistenceObject = PersistenceBox()

# - Parsing values for parameters
os1.argumentList(sys.argv)
os1.countMyParameters()

if os1.parametersSize >= 3:
    os1.parsingParameters()

# - Confirm directory exist
if os.path.exists( os1.myFile ):
    os1.checkSourceFile(1)
    
    if os1.function == 'sessionlog':
    
        # - Separate the JSON in transaction
        jsonfile1.dumpMyJsonToFile(os1.myFile)
        jsonfile1.loadJsons()

        # - Create Stage 1 persistance
        persistenceObject.createDB(os1.myFile,STAGE1_EXTENSION)
        persistenceObject.createStage(STAGE1)
    
        # - Prepare Session Collection Tuple
        jsonfile1.sessionCollection()
        jsonfile1.sessionQueryBuilder()
    
        # - Load session data in persistent layer
        persistenceObject.insertBulkDB(jsonfile1.sessionQueryArray)

    elif os1.function == 'auditlog':
    
        # - Separate the JSON in transaction
        jsonfile1.dumpMyJsonToFile(os1.myFile)
        jsonfile1.loadJsons()

        # - Create Stage 2 persistance
        persistenceObject.createDB(os1.myFile,STAGE2_EXTENSION)
        persistenceObject.createStage(STAGE2)

        # - Prepare Audit logs 
        jsonfile1.auditCollection()
        jsonfile1.auditQueryBuilder()

        # - Load data in the persistance layer
        persistenceObject.insertBulkDB(jsonfile1.auditQueryArray)

    elif os1.function == 'stage3':

        # - Set my current database
        persistenceObject.myCurrentDatabase = os1.myFile

        # - Create Stage 3 persistance
        persistenceObject.createStage(STAGE3)
        
        # - Trigger Stage3 insert
        myResult = persistenceObject.queryDB(STAGE3_QUERY1)

    elif os1.function == 'stage4':
 
        # - Set my current database
        persistenceObject.myCurrentDatabase = os1.myFile

        # - Create Stage 3 persistance
        persistenceObject.createStage(STAGE4)

        # - Trigger Stage3 insert
        myResult = persistenceObject.queryDB(STAGE4_QUERY1)

    elif os1.function == 'finalstage':

        # - Set database
        persistenceObject.myCurrentDatabase = os1.myFile

        # - Create Final stage persistance
        persistenceObject.createStage(FINAL_STAGE1)
        persistenceObject.createStage(FINAL_STAGE2)

    elif os1.function == 'datasearch':

        # - Set my current database
        persistenceObject.myCurrentDatabase = os1.myFile
        #persistenceObject.extraFile = os1.parametersList[3]
        #print persistenceObject.extraFile

        # - Start search process, get AUDIT information
        myResult = persistenceObject.queryDB(FINAL_STAGE_QUERY1)
        #print myResult 

        # - Compare with session file
        jsonfile1.buildCountQueries(myResult,FINAL_STAGE_QUERY2,FINAL_STAGE_QUERY3)
        
        # - Query and select if exist
        persistenceObject.extraFile = persistenceObject.myCurrentDatabase
        persistenceObject.myCurrentDatabase = os1.parametersList[3]
        jsonfile1.finalConsolidation(jsonfile1.countArray,persistenceObject,persistenceObject.extraFile,persistenceObject.myCurrentDatabase,FINAL_STAGE_QUERY4,FINAL_STAGE_QUERY5)

        # - Insert consolidated data
        persistenceObject.myCurrentDatabase = os1.parametersList[4]
        persistenceObject.insertBulkDB(jsonfile1.consolidatedQueries)

    elif os1.function == 'consolidation':
        db1 = DBbox('RDS','RDS','RDS',5432)
        db1.openConnection(botLogger)

        # - Create consolidation table 1
        db1.queryRDS(CONSOLIDATION_TABLE1)

        # - Create consolidation table 2
        db1.queryRDS(CONSOLIDATION_TABLE2)

        # - Create consolidation table 1 index
        db1.queryRDS(CONSOLIDATION_TABLE1_INDEX)

        # - Create consolidation table 2 indexes
        db1.queryRDS(CONSOLIDATION_TABLE2_INDEX1)
        db1.queryRDS(CONSOLIDATION_TABLE2_INDEX2)
        db1.closeConnection(botLogger)
        print("COMPLETED")

    elif os1.function == 'load_session_data':
        # - Set my current database
        persistenceObject.myCurrentDatabase = os1.myFile

        # - Select all information from the file
        myResult = persistenceObject.queryDB(SELECT_ALL_SESSION_LOG)
        #print myResult

        # - Generate session queries
        pgbox1 = postgresQueryBox()
        pgbox1.sessionLogQueryCreation(myResult,INSERT_SESSION_DATA,SELECT_ALREADY_INSERTED_SESSION_DATA,UPDATE_ALREADY_INSERTED_SESSION_DATA)

        # - Query the inserts in the DB
        db1 = DBbox('RDS','RDS','RDS',5432)
        db1.openConnection(botLogger)
        for myQueryTuple in pgbox1.finalSessionQueryList:
            selectQuery,insertQuery,updateQuery = myQueryTuple
            db1.queryRDS(selectQuery)
            if db1.resultQuery[0][0] == 0:
                db1.queryRDS(insertQuery)
            else:
                db1.queryRDS(updateQuery)

        db1.closeConnection(botLogger)
        print("COMPLETED")

    elif os1.function == 'load_audit_data':
        # - Set my current database
        persistenceObject.myCurrentDatabase = os1.myFile

        # - Start search process, get AUDIT information
        myResult = persistenceObject.queryDB(SELECT_ALL_AUDIT_LOG)
        #print myResult

        # - Generate session queries
        pgbox1 = postgresQueryBox()
        pgbox1.auditLogQueryCreation(myResult,INSERT_AUDIT_DATA,SELECT_ALREADY_INSERTED_AUDIT_DATA,UPDATE_ALREADY_INSERTED_AUDIT_DATA)
        
        # - Query the inserts in the DB
        db1 = DBbox('RDS','RDS','RDS',5432)
        db1.openConnection(botLogger)
        for myQueryTuple in pgbox1.finalAuditQueryList:
            selectQuery,insertQuery,updateQuery = myQueryTuple
            db1.queryRDS(selectQuery)
            if db1.resultQuery[0][0] == 0:
                db1.queryRDS(insertQuery)
            else:
                db1.queryRDS(updateQuery)
        db1.closeConnection(botLogger)
        print("COMPLETED")
else:
    os1.checkSourceFile(0)
    OsAgent.clearScreen()
    print(FILE_DOES_NOT_EXIST_TRUE)
    OsAgent.elegantExit()
