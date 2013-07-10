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

    def closePool(self):
        pass	

    def getLast(self,rk):
	self.status = 'bobo'
        #try:
        self.jlookup = self.jl.get(rk, column_count=1)
        print "Jlookup = %s" % (self.jlookup)
        self.status = self.jr.get(self.jlookup.values()[0], column_start='status', column_finish='status')['status']
	print self.status
        #except Exception,e:
        #    print e
        #    return e
        return self.status

    def getJobKeys(self):
	self.rks = []
        jobs = self.jl.get_range(column_count=0, filter_empty=False)
        for job in jobs:
            self.rks.append(job[0])
	return self.rks

#    def getSuccess(sefl,rk,days):
#        start = datetime.now()
#        stop = start-timedelta(days)
#        statusSum = 0.0
#        numjobs = 0
#        try:
#            jlookup = jl.get(rk, column_start=start, column_finish=stop)
#        except:
#            return None
#        job_statuses = jr.multiget(jlookup.values(), column_start='status', column_finish='status')
#        #print statuses
#        try:
##             numjobs = len(job_statuses)
#        except:
#             return 0.0
#        for status in statuses.values():
#             #if days == 7: print status['status']
#             if status['status'] == 0:
#                statusSum += 1
#        #if days == 7: print statusSum, numjobs
#        if statusSum != 0 and numjobs != 0:
#            success = (float(statusSum) / float(numjobs)) * 100
#        else:
#            success = 0.0
#        return success

#j = {}
#jobs = jl.get_range(column_count=0, filter_empty=False)
#for job in jobs:
#    rk = job[0]
##    print rk
#    last = getLast(rk)
#    success7 = getSuccess(rk, 7)
#    success14 = getSuccess(rk, 14)
#    success30 = getSuccess(rk, 30)
##    print rk, last, success7, success14, success30
#    if success7 != None and success14 != None and success30 != None:
#        j[rk] = {'last': last, 'success7': success7, 'success14': success14, 'success30': success30}
#
#print j
