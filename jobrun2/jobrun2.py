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

if __name__ == "__main__":
    app.run(debug=True)
