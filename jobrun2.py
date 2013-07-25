#!/usr/bin/env python
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa import NotFoundException
from datetime import datetime, timedelta
from uuid import uuid4
from string import *
from operator import itemgetter, attrgetter

class JobRun2:
    def __init__(self):
	err = ''
        self.pool = ConnectionPool('jobrun', server_list=['tamp20-seot-cas1.bhn.net:9160',
                                             'tamp20-seot-cas2.bhn.net:9160',
                                             'tamp20-seot-cas3.bhn.net:9160',])
        self.jl = ColumnFamily(self.pool, 'job_lookup')
        self.jr = ColumnFamily(self.pool, 'job_results')
        self.jd = ColumnFamily(self.pool, 'job_dashboard')
        self.jf = ColumnFamily(self.pool, 'job_failures')

    def closePool(self):
        pass	

    def getLast(self,rk):
        try:
            jlookup = self.jl.get(rk, column_count=1)
            status = self.jr.get(jlookup.values()[0],columns=['status']).values()[0]
        except Exception,e:
            return float(-100)
        return status

    def getToday(self, rk):
        today = datetime.strptime(datetime.today().strftime('%m/%d/%Y 00:00:00'), '%m/%d/%Y %H:%M:%S')
        tomorrow = today + timedelta(1)
        try:
            jf_count = self.jf.get_count(rk, column_start=tomorrow, column_finish=today)
        except Exception, e:
            return float(-100)
        try:
            statuses = self.jr.multiget(jlookup.values(), column_start='status', column_finish='status')
            for status in statuses.values():
                if status['status'] == 0:
                    s += 1
	        else:
		    s = 2
            success_rate = (float(s)/float(len(statuses))) * 100
        except Exception, e:
            success_rate = float(99.99)
            
        return success_rate

    def getJobKeys(self):
	rks = []
        jobs = self.jl.get_range(column_count=0, filter_empty=False)
        for job in jobs:
            rks.append(job[0])
	x = sorted(rks,key=itemgetter(0,1)) 
	#sorted(x,key=itemgetter(1),reverse=True) 
	return x 

    def getJobKey(self,rk):
	self.rk = []
	try:
            rk = self.jl.get(rk,column_count=0, filter_empty=False)
	    return self.rk
	except NotFoundException:
	    jobs = {}
	    jobs['Error'] = 'No Keys Found'
	    return jobs

    def getFailedJobUUIDs(self,rk,days):
	start = datetime.today()
        stop = start-timedelta(days)
	print start
	print stop
	rks = []
	try:
		ct = self.jf.get_count(rk,column_start=start,column_finish=stop)
		jobs = self.jf.get(rk,column_start=start,column_finish=stop,column_count=ct)
		return jobs
	except NotFoundException,e:
	    jobs = {}
	    jobs['Error'] = 'No Failed Jobs Found For Time Period'
	    return jobs

    def getJobDashboardKeys(self):
	self.rks = []
	try:
            jobs = self.jd.get_range(column_count=0, filter_empty=False)
	except NotFoundException:
	    return none
        for job in jobs:
            self.rks.append(job[0])
	return self.rks

    def getJobDashboardSuccessAll(self):
	successRates = {}
	jl_rks = self.getJobKeys()
	for key in jl_rks:
	    successRates[key] = {}
	    successRates[key][0] = self.getLast(key)
	    successRates[key][1] = self.getSuccess(key,1)
	    for days in [90,60,30]:
	    	successRates[key][days] = self.getSuccess(key,(int(days)))
	return successRates

    def getJobDashboardSuccessMulti(self,rks):
	successRates = self.jd.multiget(rks)
	return successRates

    def getJobDashboardSuccess(self,rk):
	try:
	    successRate = self.jd.get(rk)
	    return successRate
	except NotFoundException:
	    return None

    def getJobRs(self,rk):
	job_rs = self.jr.get(rk)
	return job_rs

    def getSuccess(self,rk,days):
	start = datetime.now()
    	stop = start-timedelta(days)
	statusSum = 0.0
	numjobs = 0
	try:
		jl_total = self.jl.get_count(rk, column_start=start, column_finish=stop)
    		jl_failure = self.jf.get_count(rk,column_start=start, column_finish=stop)
		failRate = (float(jl_failure) / float(jl_total)) * 100
	except:
		failRate = 100
	return (100 - failRate) 

    def insertJobRs(self,dataset,action,jobDict):
	job_uuid = uuid4()
        year = int(jobDict['started'].year)
	jl_rk = [dataset,action]
        job_results = {}
        if jobDict.has_key('status'): job_results['status'] = jobDict['status']
        if jobDict.has_key('output'): job_results['output'] = jobDict['output']
        if jobDict.has_key('command'): job_results['command'] = jobDict['command']
        if jobDict.has_key('username'): job_results['username'] = jobDict['username']
        if jobDict.has_key('program'): job_results['program'] = jobDict['program']
        if jobDict.has_key('machine'): job_results['machine'] = jobDict['machine']
	self.jl.insert(jl_rk,{jobDict['started']:job_uuid})
	self.jr.insert(job_uuid,job_results,ttl=7776000)
	if jobDict['status'] != 0:
		self.insertJobRsFailure(dataset,action,jobDict['started'],uuid)

    def insertJobRsFailure(self,dataset,action,dt,uuid):
	rk = [dataset,action]
	self.jf.insert(rk,{started:uuid},ttl=7776000)

    def insertJobDashboardSuccess(self,rk,days,successRate):
	self.jd.insert(rk,{int(days):float(successRate)})



