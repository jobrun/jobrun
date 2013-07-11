#!/usr/bin/env python
from jobrun2 import JobRun2

jr = JobRun2()

jobs = jr.getJobKeys()

for job in jobs:
    successRate = {}
    success30 = jr.getSuccess(job, 30)
    success60 = jr.getSuccess(job, 60)
    success90 = jr.getSuccess(job, 90)
    jr.insertJobDashboardSuccess(job, 30, success30)
    jr.insertJobDashboardSuccess(job, 60, success60)
    jr.insertJobDashboardSuccess(job, 90, success90)
