# db_collate.py -- Collate Oncokb, synapse, civic and docm gene databases
#
# Last Modified: Mon Aug 21 18:43:52 UTC 2017
#

import csv
import MySQLdb


# Index of each field in OncoKB mapped to standard table fmt.
#
def MapOncokb(d):
    d['Database'] = 'OncoKB'
    d['Mutation'] = 0
    d['Signature'] = 1
    d['Variant'] = 99999
    d['Functionality'] = 3
    d['Impact'] = 99999
    d['Indication'] = 99999
    d['Domain'] = 99999
    d['Classification'] = 99999
    d['Reference'] = 4

    return d




# Index of each field in Civic DB mapped to standard table fmt.
# 
def MapCivic(d):
    d['Database'] = 'Civic'
    d['Mutation'] = 0
    d['Signature'] = 2
    d['Variant'] = 3
    d['Functionality'] = 99999
    d['Impact'] = 99999
    d['Indication'] = 99999
    d['Domain'] = 99999
    d['Classification'] = 99999
    d['Reference'] = 11

    return d

# # Index of each field in Synapse DB mapped to standard table fmt.
# # 
def MapSynapse(d):
    d['Database'] = 'Synapse'
    d['Mutation'] = 1
    d['Signature'] = 2
    d['Variant'] = 3
    d['Functionality'] = 4
    d['Impact'] = 99999
    d['Indication'] = 99999
    d['Domain'] = 99999
    d['Classification'] = 99999
    d['Reference'] = 9

    return d    



# Standardise fuctionality to LOF, GOF, etc in the various DB's
def MapFunctionality(str):
    functionality = { # OncoKB map
         'Loss-of-function': 'LOF', 'Likely Loss-of-function': 'LOF',
         'Gain-of-function': 'GOF', 'Likely Gain-of-function': 'GOF',         
         'Switch-of-function': 'SOF', 'Likely Switch-of-function': 'SOF',         
         'Mutation Effect': 'COF', 'Likely Mutation Effect': 'COF',         
         'Inconclusive':  'Inconclusive',
         'Neutral':  'Neutral', 'Likely Neutral':  'Neutral',
         # 
         #synapse
         'gain-of-function': 'GOF', 'gain-of-function (low activity)': 'GOF',
         'loss-of-function' : 'LOF', 'switch-of-function': 'SOF',
         'reduced kinase activity':'LOF',
         'not applicable': 'NA',
         #
         }

    return functionality[str.strip()]




# Copy over the row from CSV to standard fmt and then write it out.
# Note: Currently simply writes to stdout.
#
def WriteDB(dbFd, dbMap, row):
    tblRow = 10*['NA']

    tblRow[0] = dbMap['Database']
    tblRow[1] = row [dbMap['Mutation']]
    tblRow[2] =  (row [dbMap['Signature']])
    if (dbMap['Variant'] != 99999):
        tblRow[3] = (row [dbMap['Variant']])
    if (dbMap['Functionality'] != 99999):
        tblRow[4] = MapFunctionality (row [dbMap['Functionality']])
    if (dbMap['Impact'] != 99999):
        tblRow[5] = row [dbMap['Impact']]
    if (dbMap['Indication'] != 99999):
        tblRow[6] = row [dbMap['Indication']]
    if (dbMap['Domain'] != 99999):
        tblRow[7] = row [dbMap['Domain']]
    if (dbMap['Classification'] != 99999):
        tblRow[8] = row [dbMap['Classification']]
    if (dbMap['Reference'] != 99999):
        tblRow[9] = row [dbMap['Reference']]

    return tblRow     



##  main()

dbFd = 1   # stdout will be the 'DB' for now.

myfile = open('final_record.csv', 'wb+')
writer = csv.writer(myfile, delimiter=',')

oncoMap = dict();
MapOncokb(oncoMap)
with open('oncoKB_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter='\t')
    for row in reader:
        lin = WriteDB(dbFd, oncoMap, row)
        writer.writerow(lin)
civicMap = dict()
MapCivic(civicMap)
with open('civic_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter='\t')
    for row in reader:
        lin = WriteDB(dbFd, civicMap, row)
        writer.writerow(lin)

synapseMap = dict()
MapSynapse(synapseMap)
with open('synapse_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter=',')
    for row in reader:
        lin = WriteDB(dbFd, synapseMap, row)
        writer.writerow(lin)
          


#Store final data in mysql database.

database = MySQLdb.connect(host='localhost', user='root', passwd='admin123')
cursor = database.cursor()
create_database = "CREATE DATABASE IF NOT EXISTS cellworks"
cursor.execute(create_database)
database = MySQLdb.connect(host='localhost', user='root', passwd='admin123', db='cellworks')
cursor = database.cursor()
create_table = "CREATE TABLE IF NOT EXISTS final_data (Data VARCHAR(255), Mutation VARCHAR(255), Signature VARCHAR(255), Varient VARCHAR(255), Functionality VARCHAR(255), Impact VARCHAR(255), Indication VARCHAR(255), Domain VARCHAR(255), Classification VARCHAR(255), Refrence VARCHAR(255))"
cursor.execute(create_table)
file  = open('final_record.csv', "rb")
reader = csv.reader(file)
for row in reader:
    cursor.execute('INSERT INTO final_data (Data,Mutation,Signature,Varient,Functionality,Impact,Indication,Domain,Classification,Refrence) VALUES("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
    #print row
database.commit()
cursor.close()