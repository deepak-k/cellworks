#db_get.py -- get all Databases from URLs and write the CSV into /tmp/DB_name
#

import csv
import MySQLdb
from urllib import urlretrieve
import os


onco_url = "http://oncokb.org/api/v1/utils/allAnnotatedVariants.txt"
print 'retrieving from' + onco_url
urlretrieve(onco_url,"/tmp/oncotxt.txt")



