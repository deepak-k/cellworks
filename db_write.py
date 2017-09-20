# db_write.py: Write final_record.csv to Database.
#
#

import csv
import MySQLdb
from datetime import date


# NOTE: Combining multiple statements and executing at one go with cur.execute() 
# fails with "ProgrammingError: (2014, "Commands out of sync; you can't run this command now") Error.
# Something to do with MySQL not able to handle multiple queries at one go.

db_conn = MySQLdb.connect(host='localhost', user='drupal', passwd='drupal123')
cur = db_conn.cursor()

qry = 'USE cellworks;'
cur.execute(qry)
qry = 'DROP TABLE IF EXISTS Mutation_Evidence'
cur.execute(qry)
qry = '''
CREATE TABLE IF NOT EXISTS Mutation_Evidence (
  evidenceNo bigint(20) NOT NULL AUTO_INCREMENT,
  mutation varchar(30) NOT NULL,
  signature varchar(100) DEFAULT NULL,
  functionality varchar(5) DEFAULT NULL,
  source varchar(100) DEFAULT NULL,
  impact varchar(25) DEFAULT NULL,
  mutationDefinitionTag varchar(200) DEFAULT NULL,
  indicationId int(3) DEFAULT NULL,
  domainName varchar(250) DEFAULT NULL,
  domainRange varchar(20) DEFAULT NULL,
  reference text,
  lastModified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (evidenceNo)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;
'''
cur.execute(qry)

file  = open('final_record.csv', "rb")
reader = csv.reader(file)
datestamp = date.today()

for row in reader:
    if len(row) == 10:
        print row
        id = str(row[6]).strip("'")
        cur.execute ('''INSERT INTO Mutation_Evidence (mutation, signature,
                       functionality, source, impact, mutationDefinitionTag, indicationId,
                       domainName, domainRange, reference, lastModified) 
                       VALUES("%s", "%s", "%s", "%s", "%s","%s", "%s","%s","%s", "%s", "%s")''',
                       (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], str(datestamp)))
    else:
        print "db_collate: Error in row len, not written to DB"
        print row

db_conn.commit()
cur.close()

