#!/usr/bin/env python

from flask import Flask, request
from uuid import uuid4
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import datetime

app = Flask(__name__)

pool = ConnectionPool('jobrun', server_list=['tamp20-seot-cas1.bhn.net:9160',
                                             'tamp20-seot-cas2.bhn.net:9160',
                                             'tamp20-seot-cas3.bhn.net:9160',])
jl = ColumnFamily(pool, 'job_lookup')
jr = ColumnFamily(pool, 'job_results')

@app.route('/jobrun2/record_job', methods=['POST'])
def record_job():
    jobresults = {}
    print request.form
    started = datetime.datetime.strptime(request.form['started'], '%m/%d/%Y %H:%M:%S.%f')
    jobresults['dataset'] = request.form['dataset']
    jobresults['action'] = request.form['action']
    jobresults['status'] = int(request.form['status'])
    jobresults['output'] = request.form['output']
    if request.form['command']:
        jobresults['command'] = request.form['command']
    if request.form['username']:
        jobresults['username'] = request.form['username']
    if request.form['program']:
        jobresults['program'] = request.form['program']
    if request.form['machine']:
        jobresults['machine'] = request.form['machine']

    jobid = uuid4()
    year = int(datetime.datetime.strftime(started, '%Y'))
    rk = (jobresults['dataset'], jobresults['action'], year)
    jl.insert(rk, {started: jobid})
    jr.insert(jobid, jobresults)
    return('OK')

@app.route('/jobrun2')
def show_jobs():
    j = {}
    jobs = jl.get_range(column_count=0)
    for job in jobs:
        rk = job[0]
        last = getLast(rk)
        success7 = getSuccess(rk, 7)
        success14 = getSuccess(rk, 14)
        success30 = getThirtyDaySuccess(rk, 30)
        j[rk] = {'last': last, 'success7': success7, 'success14': success14, 'success30': success30}

def getLast(rk):
    jlookup = jl.get(column_reversed=True, column_count=1)
    status = jr.get(jlookup.values()[0], column_start='status', column_finish='status')['status']
    return status

def getSuccess(rk, days):
    start = datetime.datetime.now()
    stop = start-datetime.timedelta(days)
    statusSum = 0
    jlookup = jl.get(column_start=start, column_finish=stop)
    statuses = jr.multiget(jlookup.volues(), column_start='status', column_finish='status')
    try:
        numjobs = len(statuses)
    except:
        return 0.0
    for status in statuses:
        statusSum += status['status']
    success = (float(statusSum) / float(numjobs)) * 100
    return success


if __name__ == "__main__":
    app.run(debug=True)
