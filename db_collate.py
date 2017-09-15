# db_collate.py -- Collate Oncokb, synapse, civic and docm gene databases
#
# Last Modified: Mon Aug 21 18:43:52 UTC 2017
#

# CHANGED COLUMN
#1) Mutation  REQD
#2) Signature  REQD
#3) Functionality  REQD
#4) Impact
#5) Mutation Definition Tag
#6) Indication ID
#7) Domain Name
#8) Domain region
#9) Reference  REQD


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
    d['Functionality'] = 99999
    d['Impact'] = 99999
    d['Mutation_definition_Tag'] = 99999
    d['Indication_ID'] = 99999
    d['Domain_Name'] = 99999
    d['Domain_Region'] = 99999
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
    d['Functionality'] = 99999
    d['Impact'] = 99999
    d['Mutation_definition_Tag'] = 99999
    d['Indication_ID'] = 99999
    d['Domain_Name'] = 99999
    d['Domain_Region'] = 99999
    d['Reference'] = 15   # 

    return d



# Index of each field in OncoKB mapped to standard table fmt.
#
def MapOncokb(d):
    d['Database'] = 'OncoKB'
    d['Mutation'] = 0
    d['Signature'] = 1   # BUG: Remove Truncating Mutation, Promoter Mutation
    d['Functionality'] = 3
    d['Impact'] = 99999
    d['Mutation_definition_Tag'] = 99999
    d['Indication_ID'] = 99999
    d['Domain_Name'] = 99999
    d['Domain_Region'] = 99999
    d['Reference'] = 4

    return d



# Index of each field in Civic DB mapped to standard table fmt.
# 
def MapCivic(d):
    d['Database'] = 'Civic'
    d['Mutation'] = 0
    d['Signature'] = 2
    d['Functionality'] = 99999
    d['Impact'] = 99999
    d['Mutation_definition_Tag'] = 99999
    d['Indication_ID'] = 99999   # Changed; mapped to 'Disease'
    d['Domain_Name'] = 99999
    d['Domain_Region'] = 99999
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
    d['Functionality'] = 4
    d['Impact'] = 99999
    d['Mutation_definition_Tag'] = 99999    # DISEASE (Column 1)
    d['Indication_ID'] = 99999
    d['Domain_Name'] = 99999
    d['Domain_Region'] = 99999
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

    # TODO: Fix so that if mapping is not present, it defaults
    # to the string sent (and log error somewhere)
    #
    return functionality[str.strip()]


# Copy over the row from CSV to standard fmt and then write it out.
# Note: Currently simply writes to stdout.
#
def FormatRow(dbFd, dbMap, row):
    tblRow = 10*['NA']

    tblRow[0] = dbMap['Database']
    try:  
        tblRow[1] = row [dbMap['Mutation']]
    except:
        print 'FormatRow: Offending row'
        print row

    if isinstance (dbMap['Signature'], list):
        tblRow[2] = dbMap['Signature'][0] (dbMap['Signature'][1:], row)
    else:
        tblRow[2] =  (row [dbMap['Signature']])
    if (dbMap['Functionality'] != 99999):
        tblRow[3] = MapFunctionality (row [dbMap['Functionality']])
    if (dbMap['Impact'] != 99999):
        tblRow[4] = row [dbMap['Impact']]
    if (dbMap['Mutation_definition_Tag'] != 99999):
        tblRow[5] = row [dbMap['Indication']]
    if (dbMap['Indication_ID'] != 99999):
        tblRow[6] = row [dbMap['Domain']]
    else:
        tblRow[6] = '0000'
    if (dbMap['Domain_Name'] != 99999):
        tblRow[7] = row [dbMap['Classification']]
    if (dbMap['Domain_Region'] != 99999):
        tblRow[7] = row [dbMap['Domain_Region']]

    if isinstance(dbMap['Reference'], list):
        tblRow[8] = dbMap['Reference'][0] (dbMap['Reference'][1:], row)
    elif (dbMap['Reference'] != 99999):
            tblRow[9] = row [dbMap['Reference']]

    return tblRow     


def FilterRow(row):
    if (row[3] == 'SOF' or row[3] == 'LOF' or row[3] == 'COF' or row[3] == 'GOF' or row[3] == 'NA'
            and (row[2] != 'Promoter Mutations' and row[2] != 'Promoter Hypermethylation' and row[2] != 'Truncating Mutations'
                and row[2] != 'Amplification' and row[2] != 'Amplification' and row[2] != 'Copy Number Loss'
                and row[2] != 'Hypermethylation' and row[2] != 'Overexpression' and row[2] != 'Deletion')
        ):
        return True
    else:
        return False



##  main()

dbFd = 1   # stdout will be the 'DB' for now.

# final_record.csv will store data from all the databases into
# one CSV.
dbfd = open('final_record.csv', 'wb+') 
db_writer = csv.writer(dbfd, delimiter=',')
lin = ['Source', 'Mutation','Signature','Functionality','Impact','Mutation_Definition_Tag','Indication_ID','Domain_Name','Domain_region','Reference']
db_writer.writerow(lin)  # Write Header.

# File with filters
filter_fd = open('filter_record.csv', 'wb+') 
filter_writer = csv.writer(filter_fd, delimiter=',')
lin = ['Source', 'Mutation','Signature','Functionality','Impact','Mutation_Definition_Tag','Indication_ID','Domain_Name','Domain_region','Reference']
filter_writer.writerow(lin)  # Write Header.


print 'processing oncoKB'
oncoMap = dict();
MapOncokb(oncoMap)
with open('oncoKB_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter='\t')
    for row in reader:
        lin = FormatRow(dbFd, oncoMap, row)
        if FilterRow(lin):
            filter_writer.writerow(lin)
        db_writer.writerow(lin)
print 'processing Civic'
civicMap = dict()
MapCivic(civicMap)
with open('civic_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter='\t')
    for row in reader:
        lin = FormatRow(dbFd, civicMap, row)
        db_writer.writerow(lin)
        if FilterRow(lin):
            filter_writer.writerow(lin)

print 'processing Synapse'
synapseMap = dict()
MapSynapse(synapseMap)
with open('synapse_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter=',')
    for row in reader:
        lin = FormatRow(dbFd, synapseMap, row)
        db_writer.writerow(lin)
        if FilterRow(lin):
            filter_writer.writerow(lin)

print 'processing docm'
docmMap = dict()
MapDocm(docmMap)
with open('docm_tmp.tsv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter='\t')
    for row in reader:
        lin = FormatRow(dbFd, docmMap, row)
        db_writer.writerow(lin)
        if FilterRow(lin):
            filter_writer.writerow(lin)

print 'processing Candl'
candlMap = dict()
MapCandl(candlMap)
with open('candl_tmp.csv', 'rb') as csvFd:
    reader = csv.reader(csvFd, delimiter=',')
    for row in reader:
        if len(row) < 10:
            print 'Error in row: '
            print row
            continue
        lin = FormatRow(dbFd, candlMap, row)
        db_writer.writerow(lin)
        if FilterRow(lin):
            filter_writer.writerow(lin)
        db_writer.writerow(lin)

dbfd.close()
filter_fd.close() 

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
    Reference VARCHAR(2048));
'''

cur.execute(qry)

file  = open('final_record.csv', "rb")
reader = csv.reader(file)

next(reader, None) # Skipping header
for row in reader:
    if len(row) == 10:
        cur.execute('''INSERT INTO final_data (Source, Mutation,Signature,
                       Variant,Functionality,Impact, Indication,Domain,
                       Classification,Reference) 
                       VALUES("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")''', 
                       (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
    else:
        print "db_collate: Error in row len, not written to DB"
        print row

db_conn.commit()
cur.close()


