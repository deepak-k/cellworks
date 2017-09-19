import csv
import MySQLdb
from urllib import urlretrieve
import os


url = "http://oncokb.org/api/v1/utils/allAnnotatedVariants.txt"
urlretrieve(url,"oncotxt.txt")

with open('oncotxt.txt', 'r') as infile, open('oncocsv.csv', 'w') as outfile:
    in_txt = csv.reader(infile, delimiter = '\t')
    
    out_csv = csv.writer(outfile)
    out_csv.writerows(in_txt)
'''with open("test.csv",'r') as f:
    with open("oncocsv.csv",'w') as f1:
        next(f) # skip header line
        for line in f:
            f1.write(line)'''
        
os.remove('oncotxt.txt')

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

def MapFunctionality(str):
    functionality = { # OncoKB map
         'Loss-of-function': 'LOF', 'Likely Loss-of-function': 'LOF',
         'Gain-of-function': 'GOF', 'Likely Gain-of-function': 'GOF',         
         'Switch-of-function': 'SOF', 'Likely Switch-of-function': 'SOF',         
         'Mutation Effect': 'COF', 'Likely Mutation Effect': 'COF',         
         'Inconclusive':  'Inconclusive',
         'Neutral':  'Neutral', 'Likely Neutral':  'Neutral', 'null': 'NA',
         # 
        
         }

    return functionality[str.strip()]


def WriteDB(dbFd, dbMap, row):
    
    tblRow = 10*['NA']

    tblRow[0] = dbMap['Database']
    tblRow[1] = row [dbMap['Mutation']]
    tblRow[2] =  row [dbMap['Signature']]
    if (dbMap['Variant'] != 99999):
        tblRow[3] = (row [dbMap['Variant']])
    if (dbMap['Functionality'] != 99999):
        tblRow[4] =  MapFunctionality (row [dbMap['Functionality']])
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
  
dbFd = 1

myfile = open('finalrecord.csv', 'w')
writer = csv.writer(myfile, delimiter=',')

oncoMap = dict()
MapOncokb(oncoMap)


with open('oncocsv.csv', 'r') as csvFd: 
    reader = csv.reader(csvFd, delimiter=',')
    next(reader)
    for row in reader:              
        lin = WriteDB(dbFd, oncoMap, row)
        writer.writerow(lin)
#os.remove('oncocsv.csv')
        

database = MySQLdb.connect(host='localhost', user='root', passwd='root')
cursor = database.cursor()
create_database = "CREATE DATABASE IF NOT EXISTS cellworksDATA"
cursor.execute(create_database)
database = MySQLdb.connect(host='localhost', user='root', passwd='root', db='cellworksDATA')
cursor = database.cursor()
create_table = "CREATE TABLE IF NOT EXISTS cellworksTABLE (Data VARCHAR(255), Mutation VARCHAR(255), Signature VARCHAR(255), Variant VARCHAR(255), Functionality VARCHAR(255), Impact VARCHAR(255), Indication VARCHAR(255), Domain VARCHAR(255), Classification VARCHAR(255), Reference VARCHAR(255))"
cursor.execute(create_table)
with open('finalrecord.csv', "r") as file:
 reader = csv.reader(file)
 for row in reader:
    if len(row) == 10:
        cursor.execute('INSERT INTO cellworksTABLE (Data,Mutation,Signature,Variant,Functionality,Impact,Indication,Domain,Classification,Reference) VALUES("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
    else:
        print('Error in row: ')
        print(row)
database.commit()
cursor.close()

os.remove('finalrecord.csv')
