#!/usr/bin/env python

from flask import Flask, request, render_template, redirect, url_for, flash
from uuid import *
import uuid
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import datetime

app = Flask(__name__)
app.secret_key = 'some_secret'

from jobrun2 import JobRun2

jr = JobRun2()

# Home status page
@app.route('/jobrun2')
def show_jobs():
    data = jr.getJobDashboardSuccessAll()
    keylist = jr.getJobKeys()
    todayYear = datetime.datetime.now().year
    return render_template('dashboard.html', data=data,keylist=keylist) 

@app.route('/jobrun2/jobfailures/<dataset>/<action>/<days>')
def jobfailures(dataset,action,days):
    job_failure_rk = [dataset,action]
    jobuuids = jr.getFailedJobUUIDs(job_failure_rk,days)
    return render_template('jobfailures.html',jobuuids=jobuuids ,job_failure_rk=job_failure_rk)

@app.route('/jobrun2/get_last_run/<dataset>/<action>')
def get_last_run(dataset,action):
    job_rk = [dataset,action]
    jobrs = jr.getLastJobrun(job_rk)
    jobuuid=0
    return render_template('jobresults.html',job_uuid=jobuuid,jobrs=jobrs)

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

@app.route('/jobrun2/task/enable/<dataset>/<action>')
def enable_task(dataset, action):
    return redirect(url_for('show_jobs'))

@app.route('/jobrun2/task/disable/<dataset>/<action>')
def disable_task(dataset, action):
    return redirect(url_for('show_jobs'))

@app.route('/jobrun2/task/edit/<dataset>/<action>')
def edit_task(dataset, action):
    rk = (dataset, action)
    flash('Task does not exist')
    return redirect(url_for('show_jobs'))

@app.route('/jobrun2/task/add')
def add_task():
    return render_template('add_task.html')

@app.route('/jobrun2/worker/add')
def add_worker():
    return render_template('add_worker.html')

@app.route('/jobrun2/worker/edit/<worker_id>')
def edit_worker(worker_id):
    return redirect(url_for('show_jobs'))

@app.route('/jobrun2/worker/delete/<worker_id>')
def delete_worker():
    return redirect(url_for('show_jobs'))

if __name__ == '__main__':
    app.run(debug=True)
