#db_get.py -- get all Databases from URLs and write the CSV into /tmp/DB_name
#

import csv
import MySQLdb
from urllib import urlretrieve
import os


onco_url = "http://oncokb.org/api/v1/utils/allAnnotatedVariants.txt"
print 'retrieving from' + onco_url
urlretrieve(onco_url,"/tmp/oncotxt.txt")

civic_url = "https://civic.genome.wustl.edu/downloads/nightly/nightly-ClinicalEvidenceSummaries.tsv"
print 'retrieving from' +civic_url
urlretrieve(civic_url, "/tmp/civic.tsv")

Synapse_url = 'https://www.synapse.org/Portal/filehandle?entityId=syn2653551&preview=false&proxy=false&xsrfToken=F7228553D367BBB7C8CE655512E8AC5D&version=1'
values = {'username': 'soumya.s@gai.co.in',
          'password': 'GAIcellworks'}
r = requests.post(url, data=values)
print 'retrieving from' + Synapse_url
urlretrieve(synapse_url,"/tmp/synapse.xls")


