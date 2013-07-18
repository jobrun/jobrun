#!/usr/bin/env python

import cx_Oracle
import sys
from uuid import uuid4
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from datetime import datetime, timedelta

db = cx_Oracle.connect('filetransfer', 'Aqu1haid', 'forge')
cur = db.cursor()
print 'Connected to forge...'

pool = ConnectionPool('jobrun', server_list=['tamp20-seot-cas1.bhn.net:9160',
                                             'tamp20-seot-cas2.bhn.net:9160',
                                             'tamp20-seot-cas3.bhn.net:9160',])

print 'Connected to cassandra...'

jl = ColumnFamily(pool, 'job_lookup2')
jr = ColumnFamily(pool, 'job_results')
jf = ColumnFamily(pool, 'job_failures')

if len(sys.argv) == 2:
    query = """SELECT STARTED, DATASET, ACTION, STATUS, OUTPUT, COMMAND, USERNAME, PROGRAM, MACHINE FROM JOBMON.JOBRUNLOG WHERE DATASET='%s'""" % sys.argv[1]
elif len(sys.argv) == 3:
    query = """SELECT STARTED, DATASET, ACTION, STATUS, OUTPUT, COMMAND, USERNAME, PROGRAM, MACHINE FROM JOBMON.JOBRUNLOG WHERE DATASET='%s' AND ACTION='%s'""" % (sys.argv[1], sys.argv[2])
else:
    print 'Incorrect number of parameters specified, please provide either DATASET or DATASET and ACTION.'
    sys.exit(1)

cur.execute(query)
row = cur.fetchone()
while not row == None:
    started = row[0]
    started_date = datetime.strptime(started.strftime('%m/%d/%Y'), '%m/%d/%Y')
    rk = (unicode(row[1]), unicode(row[2]))
    jobresults = {}
    if row[3]: jobresults['status'] = int(row[3])
    if row[4]: jobresults['output'] = row[4]
    if row[5]: jobresults['command'] = row[5]
    if row[6]: jobresults['username'] = row[6]
    if row[7]: jobresults['program'] = row[7]
    if row[8]: jobresults['machine'] = row[8]
    jobid = uuid4()
    jl.insert(rk, {started: jobid})
    jr.insert(jobid, jobresults)
    if int(row[3]) != 0:
        jf.insert(rk, {started: jobid})
    row = cur.fetchone()
