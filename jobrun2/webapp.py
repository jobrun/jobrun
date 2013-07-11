#!/usr/bin/env python

from flask import Flask, request, render_template
from uuid import uuid4
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import datetime

app = Flask(__name__)

from jobrun2 import JobRun2

jr = JobRun2()

@app.route('/jobrun2/record_job', methods=['POST'])
def record_job():
    jobresults = {}
    #print request.form
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

@app.route('/jobrun2')
def show_jobs():
    #j = {}
    #jobs = jr.getJobDashboardKeys()
    #for job in jobs:
    #    rk = job[0]
    #    last = getLast(rk)
    #    success7 = getSuccess(rk, 7)
    #    success14 = getSuccess(rk, 14)
    #    success30 = getThirtyDaySuccess(rk, 30)
    #    j[rk] = {'last': last, 'success7': success7, 'success14': success14, 'success30': success30}
    data = jr.getJobDashboardSuccessAll()
    #data = {}
    jobs = jr.getJobKeys()
    todayYear = datetime.datetime.now().year
    for job in jobs:
        print job[2]
        if int(job[2]) == int(todayYear):
            data[(job[0], job[1])][0] = jr.getLast(job)
            data[(job[0], job[1])][1] = jr.getToday(job)
            if not isinstance(data[(job[0], job[1])][1], float):
                data[(job[0], job[1])][1] = 'None'
    #        print data(job[0], job[1])
    sample_data = {('Video', 'SDV Collection Indianapolis'): {0: 'Success', 1: 100.0, 30: 92.7, 60: 98.0, 90: 99.9},
                   ('Video', 'SDV Collection Detroit'): {0: 'Fail', 1: 0.0, 30: 72.7, 60: 58.0, 90: 29.9},
                   ('Video', 'SDV Collection Bakersfield'): {0: 'Success', 1: 100.0, 30: 92.7, 60: 98.0, 90: 99.9},
                   ('Video', 'SDV Collection AAB'): {0: 'Success', 1: 100.0, 30: 92.7, 60: 98.0, 90: 99.9},
                   ('Video', 'SDV Collection Deland'): {0: 'Success', 1: 100.0, 30: 92.7, 60: 98.0, 90: 99.9},
                   ('Video', 'SDV Collection Brandon'): {0: 'Success', 1: 100.0, 30: 92.7, 60: 98.0, 90: 99.9},
                   ('Video', 'SDV Collection Fortune'): {0: 'Success', 1: 100.0, 30: 92.7, 60: 98.0, 90: 99.9},
                  }
    print data
    return render_template('dashboard.html', data=data) 

if __name__ == '__main__':
    app.run(debug=True)
