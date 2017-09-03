# db_collate.py -- Collate Oncokb, synapse, civic and docm gene databases
#
# Last Modified: Mon Aug 21 18:43:52 UTC 2017
#

import csv
import MySQLdb


# In docm sig, simply need to remove the  'p.' prefix
# Ex: p.E2419K => E2419K
#
def MapDocmSignature (pos, row):
    col = row[pos[0]]
    return col[2:]

# Use variants.tsv
def MapDocm(d):
    d['Database'] = 'Docm'
    d['Mutation'] = 7
    d['Signature'] = [MapDocmSignature, 9]   # IMPT: Remove the "p."
    d['Variant'] = 99999
    d['Functionality'] = 99999
    d['Impact'] = 99999
    d['Indication'] = 99999
    d['Domain'] = 99999
    d['Classification'] = 99999
    d['Reference'] = 11

    return d



# Candl sign is a concate of row C,D,E
#

def MapCandlSignature(lst,row):
    sig = row[lst[0]] + row[lst[1]] + row[lst[2]]
    return sig

# Use candl
def MapCandl(d):
    d['Database'] = 'Candl'
    d['Mutation'] = 1
    d['Signature'] = [MapCandlSignature, 2,3,4] # Concatination of Columns C, D & E
    d['Variant'] = 99999
    d['Functionality'] = 99999
    d['Impact'] = 99999
    d['Indication'] = 99999
    d['Domain'] = 99999
    d['Classification'] = 99999
    d['Reference'] = 15   # TODO Column 'P'

    return d



# Index of each field in OncoKB mapped to standard table fmt.
#
def MapOncokb(d):
    d['Database'] = 'OncoKB'
    d['Mutation'] = 0
    d['Signature'] = 2   # BUG: Remove Truncating Mutation, Promoter Mutation
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
    d['Indication'] = 3   # Changed; mapped to 'Disease'
    d['Domain'] = 99999
    d['Classification'] = 99999
    d['Reference'] = 11

    return d


# Synapse reference field has multiple columns with PMID's. 
# Collect PMID's from all columns
# Params:
#   lst: List of columns to pick up PMID's
#   row: CSV row
#
def MapSynapseReference (lst, row):
    refs = ''
    for pos in lst:
        refs = refs + ',' + row[pos]

    return refs



# # Index of each field in Synapse DB mapped to standard table fmt.
# # 
def MapSynapse(d):
    d['Database'] = 'Synapse'
    d['Mutation'] = 1   #  TODO: "Few mutations have multiple signatures in same cell like 
                        # ABL1 F317L/V/I/C, this means ABL1 has four signatures which are 
                        # F317L, F317V, F317I, F317C. These need to be in different rows"
    d['Signature'] = 2
    d['Variant'] = 99999  # 3
    d['Functionality'] = 4
    d['Impact'] = 99999
    d['Indication'] = 99999    # DISEASE (Column 1)
    d['Domain'] = 99999
    d['Classification'] = 99999
    d['Reference'] = [MapSynapseReference, 9,14,19,24,29,38,43]  # PMID cols: J,O,T,Y,AD,AN,AS

    return d    



# Standardise fuctionality to LOF, GOF, etc in the various DB's
def MapFunctionality(str):
    functionality = { # OncoKB map
         'Loss-of-function': 'LOF', 'Likely Loss-of-function': 'Likely LOF',
         'Gain-of-function': 'GOF', 'Likely Gain-of-function': 'Likely GOF',         
         'Switch-of-function': 'SOF', 'Likely Switch-of-function': 'Likely SOF',         
         'Mutation Effect': 'COF', 'Likely Mutation Effect': 'Likely COF',         
         'Inconclusive':  'Inconclusive',
         'Neutral':  'COF', 'Likely Neutral':  'COF',
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
    if isinstance (dbMap['Signature'], list):
        tblRow[2] = dbMap['Signature'][0] (dbMap['Signature'][1:], row)
    else:
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
    if isinstance(dbMap['Reference'], list):
        tblRow[9] = dbMap['Reference'][0] (dbMap['Reference'][1:], row)
    elif (dbMap['Reference'] != 99999):
            tblRow[9] = row [dbMap['Reference']]

    return tblRow     



##  main()

dbFd = 1   # stdout will be the 'DB' for now.

myfile = open('final_record.csv', 'wb+')
writer = csv.writer(myfile, delimiter=',')

'''
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

docmMap = dict()
MapDocm(docmMap)
with open('docm_tmp.tsv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter='\t')
    for row in reader:
        lin = WriteDB(dbFd, docmMap, row)
        writer.writerow(lin)
'''

candlMap = dict()
MapCandl(candlMap)
with open('candl_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter=',')
    for row in reader:
        lin = WriteDB(dbFd, candlMap, row)
        writer.writerow(lin)
         
myfile.close()

#Store final data in mysql database.

db_conn = MySQLdb.connect(host='localhost', user='drupal', passwd='drupal123')
cur = db_conn.cursor()

# NOTE: Combining multiple statements and executing at one go with cur.execute() 
# fails with "ProgrammingError: (2014, "Commands out of sync; you can't run this command now") Error.
# Something to do with MySQL not able to handle multiple queries at one go.
qry = 'USE cellworks;'
cur.execute(qry)
qry = 'DROP TABLE IF EXISTS final_data'
cur.execute(qry)
qry = '''
CREATE TABLE final_data (
    Source VARCHAR(255), 
    Mutation VARCHAR(255), 
    Signature VARCHAR(255), 
    Variant VARCHAR(255), 
    Functionality VARCHAR(255), 
    Impact VARCHAR(255), 
    Indication VARCHAR(255), 
    Domain VARCHAR(255), 
    Classification VARCHAR(255), 
    Reference VARCHAR(255));
'''

cur.execute(qry)


file  = open('final_record.csv', "rb")
reader = csv.reader(file)
for row in reader:
    print row
    if len(row) == 10:
        cur.execute('''INSERT INTO final_data (Source, Mutation,Signature,
                       Variant,Functionality,Impact, Indication,Domain,
                       Classification,Reference) 
                       VALUES("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")''', 
                       (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
    else:
        print "db_collate: Error in row len, not written to DB"
        print row

    #print row
db_conn.commit()
cur.close()


