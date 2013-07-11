#!/usr/bin/env python
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from datetime import datetime, timedelta
from uuid import uuid4

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
	print rk
        try:
            jlookup = self.jl.get(rk, column_count=1)
            print "Jlookup = %s" % (jlookup)
            status = self.jr.get(jlookup.values()[0], column_start='status', column_finish='status')['status']
	    print "status = %s,%s" % (__package__,status)
        except Exception,e:
            return e
        return status

    def getToday(self, rk):
        today = datetime.strptime(datetime.today().strftime('%m/%d/%Y 00:00:00'), '%m/%d/%Y %H:%M:%S')
        tomorrow = today + timedelta(1)
        s = 0
        try:
            jlookup = self.jl.get(rk, column_start=tomorrow, column_finish=today)
        except Exception, e:
            return e
        try:
            statuses = self.jr.multiget(jlookup.values(), column_start='status', column_finish='status')
        except Exception, e:
            return e
            
        for status in statuses.values():
            if status['status'] == 0:
                s += 1
        #print s
        #print len(statuses)
        success_rate = (float(s)/float(len(statuses))) * 100
        return success_rate

    def getJobKeys(self):
	self.rks = []
        jobs = self.jl.get_range(column_count=0, filter_empty=False)
        for job in jobs:
            self.rks.append(job[0])
	return self.rks

    def getJobKey(self,rk):
	self.rk = []
	try:
            rk = self.jl.get(rk,column_count=0, filter_empty=False)
	    return self.rk
	except NotFoundException:
	    return None  

    def getJobDashboardKeys(self):
	self.rks = []
        jobs = self.jd.get_range(column_count=0, filter_empty=False)
        for job in jobs:
            self.rks.append(job[0])
	return self.rks

    def getJobDashboardSuccessAll(self):
	rks = self.getJobDashboardKeys()
	successRates = self.jd.multiget(rks)
	return successRates

    def getJobDashboardSuccessMulti(self,rks):
	successRates = self.jd.multiget(rks)
	return successRates

    def getJobDashboardSuccess(self,rk):
	successRate = self.jd.get(rk)
	return successRate

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
	self.jd.insert(rk,{str(days):float(successRate)})



