#!/usr/bin/env python
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from datetime import datetime, timedelta

class JobRun2:
    def __init__(self):
	err = ''
        self.pool = ConnectionPool('jobrun', server_list=['tamp20-seot-cas1.bhn.net:9160',
                                             'tamp20-seot-cas2.bhn.net:9160',
                                             'tamp20-seot-cas3.bhn.net:9160',])
        self.jl = ColumnFamily(self.pool, 'job_lookup')
        self.jr = ColumnFamily(self.pool, 'job_results')
        self.jd = ColumnFamily(self.pool, 'job_dashboard')

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
        print s
        print len(statuses)
        success_rate = (float(s)/float(len(statuses))) * 100
        return success_rate

    def getJobKeys(self):
	self.rks = []
        jobs = self.jl.get_range(column_count=0, filter_empty=False)
        for job in jobs:
            self.rks.append(job[0])
	return self.rks

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

    def insertJobResults(self,dataset,action,jobDict):
	year = date.today().year
	rk = [dataset,action,str(year)]
	self.jr.insert(rk,jobDict)

    def insertJobSuccess(self,rk,days,successRate):
	self.jd.insert(rk,{str(days):float(successRate)})


