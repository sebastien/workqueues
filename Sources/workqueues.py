#!/usr/bin/env python
# -----------------------------------------------------------------------------
# Project   : Workqueues
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : Revised BSD License
# -----------------------------------------------------------------------------
# Creation  : 21-Jun-2012
# Last mod  : 26-Jun-2012
# -----------------------------------------------------------------------------

import os, threading, subprocess, time, sys

__doc__ = """\
"""

# Hooks for custom logging functions
log  = lambda *_:sys.stdout.write(" -  %s\n" % (" ".join(map(str,_))))
warn = lambda *_:sys.stdout.write("WRN %s\n" % (" ".join(map(str,_))))
err  = lambda *_:sys.stderr.write("ERR %s\n" % (" ".join(map(str,_))))

# Execution modes for jobs/workers
AS_FUNCTION      = "function"
AS_THREAD        = "thread"
AS_PROCESS       = "process"

# States for jobs
JOB_SUBMITTED    = "submitted"
JOB_RESUBMITTED  = "resubmitted"
JOB_SELECTED     = "selected"
JOB_IN_PROCESS   = "inprocess"
JOB_REMOVED      = "removed"

# -----------------------------------------------------------------------------
#
# FREQUENCY
#
# -----------------------------------------------------------------------------

class Frequency:

	@staticmethod
	def Days(count=1):
		return Frequency._Format("D", count)

	@staticmethod
	def Weeks(count=1):
		return Frequency._Format("W", count)

	@staticmethod
	def Months(count=1):
		return Frequency._Format("M", count)

	@staticmethod
	def Years(count=1):
		return Frequency._Format("Y", count)

	@staticmethod
	def _Format(letter,count=1):
		if count == 1: return letter
		else: return letter + str(count)

# -----------------------------------------------------------------------------
#
# RESULTS
#
# -----------------------------------------------------------------------------

class Result:

	def __init__( self, value ):
		self.duration = -1
		self.started  = None
		self.value    = value

	def isSuccess( self ):
		return False

	def isFailure( self ):
		return False

	def isTimeout( self ):
		return False

	def happenedAfter( self, t ):
		return self.started >= t

	def happenedBefore( self, t ):
		return self.started < t

class Success(Result):

	def __init__( self, value ):
		Result.__init__(self, value)

	def isSuccess( self ):
		return True

class Failure(Result):

	def __init__( self, description=None, value=None, job=None):
		Result.__init__(self, value)
		self.description = description
		self.job         = job

	def isFailure( self ):
		return True

	def __str__(self):
		if self.value:
			return "%s:%s (%s) in %s:%s" % (self.__class__.__name__, self.description, self.value, self.job.__class__.__name__, self.job.id)
		else:
			return "%s:%s in %s:%s" % (self.__class__.__name__, self.description, self.job.__class__.__name__, self.job.id)

class Timeout(Failure):

	def __init__( self, value ):
		Failure.__init__(self, value)

	def isTimeout( self ):
		return True

# -----------------------------------------------------------------------------
#
# JOB
#
# -----------------------------------------------------------------------------

class Job:
	"""
	- `timeout`   indicates how long (in s) the job can run without being stopped
	- `scheduled` is a (Y,M,d,h,m) UTC tuple telling when the job should be started
	- `until`     is a (Y,M,d,h,m) UTC tuple after which the job should not be started
	- `frequency` either a number (frequency in seconds) or one of (w)eek (d)ay (m)onth.
	- `repeat`    the number of times the job should be repeated.
	- `retries`   the number of times the job was tried to run in the current cycle
	"""

	DATA          = []
	RUN           = AS_FUNCTION
	RETRIES       = 5
	RETRIES_DELAY = (60 * 1, 60 * 5, 60 * 10, 60 * 15)

	def __init__( self ):
		self.timeout   = -1
		self.scheduled = -1
		self.submitted = -1
		self.until     = -1
		self.frequency = None
		self.repeat    = -1
		self.id        = None
		self.retries   = 0
		self.lastRun   = -1
		assert "id" not in self.DATA, "DATA does not allow an 'id' attribute"

	def getRunType( self ):
		"""Returns the run type of this job, as defined by the job's `RUN` attribute"""
		return self.RUN

	def setID( self, jobID ):
		"""Sets the ID that is used to represent this job in the queue it's added to.
		The consequence of this is that a job cannot be added to multiple queues at
		the same time."""
		assert self.id is None, "Job already has a job ID."
		self.id = jobID
		return self

	def canRetry( self ):
		"""Tells if this job can be retried (ie. `self.retries < self.RETRIES`)"""
		return self.retries < self.RETRIES

	def getRetryDelay( self ):
		"""Returns the delay in seconds until the next retry"""
		i = max(0, min(len(self.RETRIES_DELAY), self.retries) - 1)
		return self.RETRIES_DELAY[i]

	def export( self ):
		data = {}
		base = dict(
			timeout   = self.timeout,
			scheduled = self.scheduled,
			until     = self.until,
			frequency = self.frequency,
			repeat    = self.repeat,
			data      = data
		)
		for field in self.DATA: data[field] = getattr(self, field)
		return base
	
# -----------------------------------------------------------------------------
#
# WORKER
#
# -----------------------------------------------------------------------------

class Worker:

	def __init__( self ):
		self.job = None
		self.onJobEnd = []

	def setJob( self, job, onJobEnd=None ):
		"""Workers can only be assigned one job at a time."""
		assert self.job is None, "Worker already has a job assigned"
		self.onJobEnd = []
		if onJobEnd: self.onJobEnd.append(onJobEnd)
		self.job = job
		return self

	def run( self, runType=None ):
		assert not self.isAvailable(), "No job set for this worker"
		# NOTE: Here the run method CAN NOT FAIL or TIMEOUT. It ALWAYS have to
		# return a result, which is either a Success, Failure or Timeout.
		# In practive, if run_type is `AS_FUNCTION` then it can actually
		# fail and timeout, but for other run types, it won't.
		run_type   = runType or self.job.getRunType()
		assert run_type in (AS_FUNCTION, AS_THREAD, AS_PROCESS), "Unkown run type for worker: %s" % (repr(run_type))
		if   run_type == AS_FUNCTION:
			result = self._runAsFunction(self.job)
		elif run_type == AS_THREAD:
			result = Failure("Job run type %s not implemented" % (self.runType))
		elif run_type == AS_PROCESS:
			result = Failure("Job run type %s not implemented" % (self.runType))
		else:
			result = Failure("Uknown job run type: %s" % (self.runType))
		# NOTE: We cannot
		return result

	def doJobEnd( self, result ):
		for callback in self.onJobEnd:
			try:
				callback(result, self)
			except Exception, e:
				err("Callback failed on worker's job end: %s for %s in %s" % (callback, result, self))

	def _setResultTime( self, result, startTime ):
		end_time = time.time()
		assert isinstance(result, Result)
		result.started  = startTime
		result.duration = end_time - startTime

	def _runAsFunction( self, job ):
		result = None
		start_time = time.time()
		try:
			result = Success(job.run())
		except Exception, e:
			result = Failure(e)
		self._setResultTime(result, start_time)
		# We have to put the doJobEnd here, as callbacks might fail or 
		# take too long
		self.doJobEnd(result)
		return result

	def _runAsThread( self, job ):
		pass

	def _runAsProcess( self, job ):
		pass

	def isAvailable( self ):
		return self.job is None

# -----------------------------------------------------------------------------
#
# WORKER POOL
#
# -----------------------------------------------------------------------------

class Pool:
	"""Pools are used to limit the number of elements (workers executing)
	at once."""

	def __init__(self, capacity):
		self.capacity   = capacity
		self._semaphore = threading.BoundedSemaphore(self.capacity)
		self._workersCount = 0

	def submit(self, job, block=False):
		if self.canAdd() or block:
			# NOTE: This is only blocking if canAdd is False
			self._semaphore.acquire()
			self._workersCount += 1
			return Worker().setJob(job, self._onWorkerJobEnd)
		else:
			return None

	def _onWorkerJobEnd( self, worker, job ):
		"""Callback called when a workers's job has ended (wether with a 
		success or failure)"""
		self._semaphore.release()
		self._workersCount -= 1

	def canAdd(self):
		"""Tells if a new worker can be started in this pool"""
		return self.count() < self.capacity

	def count(self):
		return self._workersCount

# -----------------------------------------------------------------------------
#
# SCHEDULER
#
# -----------------------------------------------------------------------------

class Scheduler:

	def __init__( self ):
		pass
			
	def select( self, queue ):
		pass

	def run( self ):
		pass

# -----------------------------------------------------------------------------
#
# INCIDENT
#
# -----------------------------------------------------------------------------

class Incident:
	"""An incident collects failures of the same type and can trigger callbacks
	when a certain amount is reached within a certain period of time"""

	# Keeps only the last 1,000 failures
	MAX_FAILURE          = 1000
	# Keeps only failures that happened during the last week
	MAX_FAILURE_LIFETIME = 60 * 60 * 24 * 7

	# If we have 5 incidents withing 15 minutes, then we trigger the incident
	FAILURE_PERIOD       = 60 * 15
	FAILURE_COUNT        = 5
	
	@staticmethod
	def GetJobTag( job ):
		if job:
			return job.__class__.__name__
		else:
			return None

	@staticmethod
	def GetFailureTag( failure ):
		if failure:
			return failure.__class__.__name__  + ":" + str(failure.description)
		else:
			return None

	def __init__( self, job=None, failure=None ):
		self.jobTag           = Incident.GetJobTag(job)
		self.failureTag       = Incident.GetFailureTag(failure)
		# TODO: The idea of level is how many consecutive periods the incident
		# has with errors.
		self.level            = 0
		self.failures         = []
		self.onAboveThreshold = []

	def matches( self, job, failure ):
		"""Tells if this incident matches the given job and failure"""
		return (self.jobTag     is None or Incident.GetJobTag(job)         == self.jobTag) and \
		        self.failureTag is None or Incident.GetFailureTag(failure) == self.failureTag

	def compact( self ):
		"""Removes the failures that exceed MAX_FAILURE_LIFETIME or MAX_FAILURE"""
		t        = time.time() - self.MAX_FAILURE_LIFETIME
		failures = filter(lambda _:_.happenedAfter(t), self.failures)
		if len(failures) >  self.MAX_FAILURE:
			failures = failures[len(failures) - self.MAX_FAILURE:]
		self.failures = failures
		return self.failures

	def log( self, job, failure ):
		"""Logs the given job and failure to the given incident"""
		if self.matches(job, failure):
			self.failures.append(failure)
			if self.isAboveThreshold():
				self.doAboveThreshold()
			return True
		else:
			return False

	def failuresWithinPeriod( self, period=None ):
		"""Returns the failures that happened within the last period of this
		incident"""
		period = period or self.FAILURE_PERIOD
		t      = time.time() - period
		return filter(lambda _:_.happenedAfter(t), self.failures)

	def isAboveThreshold( self ):
		"""Tells if there was more than `self.FAILURE_COUNT` during the
		last `self.FAILURE_PERIOD` seconds."""
		return len(self.failuresWithinPeriod()) > self.FAILURE_COUNT

	def doAboveThreshold( self ):
		for callback in self.onAboveThreshold:
			try:
				callback(self)
			except Exception, e:
				err("Exception in incident's callback: %s" % (e))
	
	def __str__( self ):
		return "%s:%s" % (self.jobTag, self.failureTag)

# -----------------------------------------------------------------------------
#
# QUEUE
#
# -----------------------------------------------------------------------------

# FIXME: Separate MemoryQueue of the abstract queue
class Queue:

	# FIXME: Add support for that
	MAX_JOBS       = 128000
	# The queue is cleaned up after a given period in seconds
	CLEANUP_PERIOD = 60 * 5

	def __init__( self ):
		self.pool      = None
		self.jobs      = []
		self.incidents = []
		self._lastSelected = -1
		self.lastCleanup = -1

	def setPool( self, pool ):
		self.pool = pool

	def clean( self ):
		"""Executes periodic cleaning up operations on the queue"""
		self.incidents    = filter(lambda _:_.clean(), self.incidents)
		self.lastCleanup = time.time()

	def submit( self, job ):
		"""Submit a new job or a list of jobs in this queue"""
		if type(job) in (tuple, list):
			return map(self.submit, job)
		return self._addJob(job)

	def resubmit( self, job ):
		assert job.id != None, "You cannot resumbit a job without an id: %s" % (job)
		# We increase the number of retries in the job
		job.retries += 1
		self._updateJobStatus(job, JOB_RESUBMITTED)
		self._readdJob(job)

	def remove( self, job ):
		"""Removes the job from the queue"""
		assert job.id != None
		self._updateJobStatus(job, JOB_REMOVED)
		self._removeJob(job)
		return job

	def list( self, until=None, since=None, status=None ):
		"""Lists the jobs in the job queue, by ascending chronological order"""
		pass

	def iterate( self, count=-1 ):
		while (count == -1 or count > 0) and self._hasJobs():
			# Takes the next available job
			job = self._getNextJob()
			# Makes sure it's time to execute it
			# ...if not, we return the time we have to wait up until the next event
			# Makes sure the pool can process the event
			# ...if not, we return the maximum time in which the pool will be free/or a callback to when the pool will be free
			worker = self.pool.submit(job, block=True)
			# Now we have the worker and we ask it to run the job
			result = worker.run()
			if   isinstance(result, Success):
				# the job is successfully processed
				self._onJobSucceeded(job)
			elif isinstance(result, Failure):
				self._onJobFailed(job, result)
			else:
				self._onJobFailed(job, UnexpectedResult(result))
			if count > 0: count -= 1
			# Takes care of cleaning up the queue if it's necessary
			now = time.time()
			if self.lastCleanup == -1:
				self.lastCleanup = now
			elif (self.lastCleanup - now) > self.CLEANUP_PERIOD:
				self.cleanup()
				self.lastCleanup = now
			# And finally returns the result
			yield result

	def run( self, count=-1 ):
		iteration = 0
		for result in self.iterate(count):
			iteration += 1

	def _getNextJob( self ):
		"""Returns the next job and sets it as selected in this queue"""
		next_job = (self._lastSelected + 1) % len(self.jobs)
		job = self._getJob(next_job)
		self._lastSelected = next_job
		self._updateJobStatus(job, JOB_SELECTED)
		return job

	def _hasJobs( self ):
		"""Tells if there are still jobs submitted in the queue"""
		return self.jobs

	def _addJob( self, job ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		# FIXME: Should be synchronized
		self.jobs.append(job)
		return job.setID("%s@%s" % (len(self.jobs) - 1, time.time())).id

	def _readdJob( self, job ):
		"""Adds an existing job to the queue and returns its ID (assigned by the queue)"""
		# FIXME: Should be synchronized
		self.jobs.append(job)
		return job.id

	def _removeJob( self, job ):
		# NOTE: This is pretty simple, but should be optimized for big queues
		self.jobs.remove(job)

	def _onJobSucceeded( self, jobOrJobID ):
		i = self._id(jobOrJobID)
		self._job(jobOrJobID).retries = 0
		self._lastProcessed = i
		del self.jobs[i]
		return i

	def _onJobFailed( self, job, failure ):
		"""Called when a job has failed."""
		incident    = self._getIncident(job, failure)
		failure.job = job
		if incident.isAboveThreshold():
			# If the incident is above threshold, we won't retry the job,
			# as it's likely to fail again
			warn(self.__class__.__name__, ": job removed as incident is above threshold:", job.id, ":", incident)
			self.remove(job)
		elif job.canRetry():
			# If the incident is not above threshold, and the job has not
			# reached its retry count, we resubmit it
			warn(self.__class__.__name__, ": job resubmitted:", job.id, "/", job.retries)
			self.resubmit(job)
		else:
			# Otherwise we remove the job from the queue
			warn(self.__class__.__name__, ": job reached maximum retries:", job.id, "/", job.retries)
			self.remove(job)
		return incident

	def _getIncident( self, job, failure ):
		"""Returns and incident that matches the job and failure"""
		for incident in self.incidents:
			if incident.log(job, failure):
				return incident
		incident = Incident(job,failure)
		self.incidents.append(incident)
		return incident

	def _updateJobStatus( self, jobOrJobID, status ):
		"""Updates the status of this job."""

	def _getJob( self, jobOrJobID ):
		"""Gets the job with the given id from the job queue."""
		i = self._id(jobOrJobID)
		return self.jobs[i]

	def _id( self, jobOrJobID ):
		if isinstance(jobOrJobID, Job):
			res = jobOrJobID.id
			assert res is not None, "Job has no id"
			return res
		else:
			return jobOrJobID 

	def _job( self, jobOrJobID ):
		return self.jobs[self._id(jobOrJobID)]

# -----------------------------------------------------------------------------
#
# DIRECTORY QUEUE
#
# -----------------------------------------------------------------------------

class DirectoryQueue(Queue):

	def __init__( self, path ):
		Queue.__init__(self)
		self.path = path
		# We create the directory if it does not exist
		if not os.path.exists(path):
			os.makedirs(path)

# -----------------------------------------------------------------------------
#
# BEANSTALK QUEUE
#
# -----------------------------------------------------------------------------

class BeanstalkQueue(Queue):
	pass

# -----------------------------------------------------------------------------
#
# ZEROMQ QUEUE
#
# -----------------------------------------------------------------------------

class ZMQueue(Queue):
	pass

# -----------------------------------------------------------------------------
#
# HTTP QUEUE
#
# -----------------------------------------------------------------------------

class HTTPQueue(Queue):
	pass

# EOF - vim: tw=80 ts=4 sw=4 noet
