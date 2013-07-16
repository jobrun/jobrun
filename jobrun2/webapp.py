#!/usr/bin/env python

from flask import Flask, request, render_template
from uuid import *
import uuid
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import datetime

app = Flask(__name__)

from jobrun2 import JobRun2

jr = JobRun2()

# Home status page
@app.route('/jobrun2')
def show_jobs():
    data = jr.getJobDashboardSuccessAll()
    jobs = jr.getJobKeys()
    todayYear = datetime.datetime.now().year
    for job in jobs:
        if data.has_key(job):
            data[job][0] = jr.getLast(job)
            data[job][1] = jr.getToday(job)
        else:
            data[job] = {}
            data[job][0] = jr.getLast(job)
            data[job][1] = jr.getToday(job)
        if not isinstance(data[(job[0], job[1])][1], float):
            data[(job[0], job[1])][1] = 'None'
    return render_template('dashboard.html', data=data) 

@app.route('/jobrun2/jobfailures/<job_failure_rk>')
def jobfailures(job_failure_rk):
    jobuuids = jr.getFailedJobUUIDs(job_failure_rk)
    return render_template('jobfailures.html',jobuuids=jobuuids ,job_failure_rk=job_failure_rk)

@app.route('/jobrun2/jobresults/<job_uuid>')
def jobresults(job_uuid):
    job_uuid = uuid.UUID(job_uuid)
    jobrs = jr.getJobRs(job_uuid)
    return render_template('jobresults.html', jobrs=jobrs,job_uuid=job_uuid )

@app.route('/jobrun2/record_job', methods=['POST'])
def record_job():
    jobresults = {}
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

    rk = (jobresults['dataset'], jobresults['action'])
    jr.insertJobResults(rk, jobresults)
    return('OK')

if __name__ == '__main__':
    app.run(debug=True)
