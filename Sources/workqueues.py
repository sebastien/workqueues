#!/usr/bin/env python
# -----------------------------------------------------------------------------
# Project   : Workqueues
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : Revised BSD License
# -----------------------------------------------------------------------------
# Creation  : 21-Jun-2012
# Last mod  : 15-Nov-2012
# -----------------------------------------------------------------------------

import os, threading, subprocess, time, datetime, sys, json, traceback
import cStringIO as StringIO
try:
	from   retro.core import asJSON
except ImportError:
	import json.dumps as     asJSON

# TODO: Add Runner 1) Inline 2) Thread 3) Process 4) TMUX
# TODO: Generator support in Job's run() to divide work?
# TODO : Add support for storing the job's results -- queues should probably
# have different queues: backburner (for not schedules), incoming:(to be processed),
# processing(being processed), failed:(jobs failed and permanently removed), completed:(completed)
# FIXME: Make sure that exceptions are well caught everywhere. For instance
# DirectoryQueue will just fail if it cannot decode the JSON

__version_ = "0.6.0"
__doc__    = """\
"""

# Hooks for custom logging functions
log  = lambda *_:sys.stdout.write(" -  %s\n" % (" ".join(map(str,_))))
warn = lambda *_:sys.stdout.write("WRN %s\n" % (" ".join(map(str,_))))
err  = lambda *_:sys.stderr.write("ERR %s\n" % (" ".join(map(str,_))))

# Execution modes for jobs/workers
AS_FUNCTION      = "function"
AS_THREAD        = "thread"
AS_PROCESS       = "process"

QUEUE_INCOMING   = "incoming"
QUEUE_IN_PROCESS = "running"
QUEUE_FAILED     = "failed"
QUEUE_COMPLETED  = "completed"
# States for jobs
# JOB:
# 
# 	STATES = dict(
# 		"submitted"   : ("resubmitted", "completed", "failed")
# 		"resubmitted" : ("resubmitted", "completed", "failed")
# 		"failed"      : None
# 		"completed""  : None
# 	)

JOB_SUBMITTED    = "submitted"
JOB_RESUBMITTED  = "resubmitted"
JOB_IN_PROCESS   = "inprocess"
JOB_COMPLETED    = "completed"
JOB_FAILED       = "failed"
JOB_REMOVED      = "removed"
JOB_CLASSES      = {}

def timestamp():
	"""Returns the current time as an UTC timestamp in seconds"""
	return time.time() - time.timezone

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

	@classmethod
	def Import( self, data ):
		result_class    = eval(data["type"])
		result          = result_class(data["value"])
		result.duration = data["duration"]
		result.started  = data["started"]
		return result

	def __init__( self, value ):
		self.duration = -1
		self.started  = timestamp()
		self.value    = value

	def isSuccess( self ):
		return False

	def isFailure( self ):
		return False

	def isTimeout( self ):
		return False

	def happenedAfter( self, t ):
		assert type(t) is float, "The given time should be given in seconds since UTC"
		return self.started >= t

	def happenedBefore( self, t ):
		assert type(t) is float, "The given time should be given in seconds since UTC"
		return self.started < t

	def export( self ):
		return dict(
			type=self.__class__.__name__.split(".")[-1],
			value=self.value,
			started=self.started,
			duration=self.duration
		)

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
		res = "%s:<%s>" % (self.__class__.__name__, self.description)
		if self.value: res += " (%s)"     % (self.value)
		if self.job:   res += " in %s:%s" % (self.job.__class__.__name__, self.job.id)
		return res

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

	@classmethod
	def Registered( cls, jobClass ):
		cls.Register(jobClass)
		return jobClass

	@classmethod
	def Register( cls, *jobClasses ):
		for job_class in jobClasses:
			name = job_class.__module__ + "." + job_class.__name__.split(".")[-1]
			if not JOB_CLASSES.has_key(name):
				JOB_CLASSES[name] = job_class

	@classmethod
	def GetClass( cls, name ):
		# NOTE: Here, we could simply use `eval` os even sys.modules to resolve
		# the class, but by doing this we rely on explicit job registration.
		# The main reason is that jobs might be insecure, so we want to limit
		# the possibility to execute arbitrary code.
		if JOB_CLASSES.has_key(name):
			return JOB_CLASSES[name]
		else:
			try:
				job_class = eval(name)
				return job_class
			except NameError:
				return None

	@classmethod
	def Import( cls, export, jobID=None ):
		"""Imports a job that was previously exported."""
		job_class = cls.GetClass(export["type"])
		if job_class:
			job       = job_class()
		else:
			job       = Job()
			job.isUnresolved = export
		# NOTE: The following list is the list of all the Job's properties
		for _ in ["timeout", "scheduled", "submitted", "until", "frequency", "repeat", "id", "retries", "lastRun", "type", "status"]:
			if export.has_key(_):
				setattr(job, _, export[_])
		if jobID: job.id  = jobID
		if export.get("result"):
			result_json = export["result"]
			result = Result.Import(result_json)
			job.result = result
		for _,v in export["data"].items():
			setattr(job, _, v)
		return job

	def __init__( self, env=None ):
		self.timeout      = -1
		self.scheduled    = -1
		self.submitted    = -1
		self.until        = -1
		self.frequency    = None
		self.repeat       = -1
		self.id           = None
		self.retries      = 0
		self.lastRun      = -1
		self.progress     = -1
		self.env          = env
		self.isUnresolved = False
		self.type         = self.__class__.__module__ + "." + self.__class__.__name__.split(".")[-1]
		self.status       = None
		self.result       = None
		assert "id" not in self.DATA, "DATA does not allow an 'id' attribute"

	def setStatus( self, status ):
		self.status = status

	def updateProgress( self, progress ):
		self.progress = progress

	def isComplete( self ):
		return self.result and isinstance(self.result, Success)

	def hasFailed( self ):
		return self.result and isinstance(self.result, Failure)

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

	def setResult( self, result ):
		"""Sets the result obtained from running this job, wrapping it in 
		a Result instance if necessary."""
		if not isinstance(result, Result): result = Result(result)
		self.result = result
		return result

	def canRetry( self ):
		"""Tells if this job can be retried (ie. `self.retries < self.RETRIES`)"""
		return self.retries < self.RETRIES

	def getRetryDelay( self ):
		"""Returns the delay in seconds until the next retry"""
		i = max(0, min(len(self.RETRIES_DELAY), self.retries) - 1)
		return self.RETRIES_DELAY[i]

	def run( self ):
		"""The main Job function that you'll override when implementing the Job"""
		raise Exception("Job.run not implemented")

	def shell( self, command, cwd="." ):
		if type(command) in (tuple, list): command = " ".join(command)
		cmd      = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, env=dict(LANG="C"))
		status   = cmd.wait()
		res, err = cmd.communicate()
		if status == 0:
			return res
		else:
			return err

	def export( self ):
		# If the job is unresolved, then we export the same data that was
		# imported
		if self.isUnresolved: return self.isUnresolved
		data = {}
		result_export = None
		if self.result:
			result_export = self.result.export()
		base = dict(
			type      = self.type,
			timeout   = self.timeout,
			scheduled = self.scheduled,
			until     = self.until,
			frequency = self.frequency,
			repeat    = self.repeat,
			data      = data,
			status    = self.status,
			result    = result_export
		)
		for field in self.DATA: data[field] = getattr(self, field)
		return base
	
	def __str__( self ):
		data = asJSON(self.export()["data"])
		if len(data) > 80: data = data[:77] + "..."
		if self.isUnresolved:
			return "%s[UNRESOLVED](%s):%s" % (self.isUnresolved["type"], self.id, data)
		else:
			return "%s(%s):%s" % (self.__class__.__name__, self.id, data)

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
		# The job is assigned a result
		self.job.setResult(result)
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
			error_msg = StringIO.StringIO()
			traceback.print_exc(file=error_msg)
			error_msg = error_msg.getvalue()
			result = Failure(error_msg, job=job)
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

	def __init__(self, capacity=5):
		"""Creates a default pool with the given capacity (5 by default)"""
		self.capacity     = capacity
		self._semaphore   = threading.BoundedSemaphore(self.capacity)
		self._workersCount = 0

	def submit(self, job, block=False):
		"""Submits a job to the pool. This will create a worker and
		assign the job to the worker."""
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
		"""Returns the number of workers in the queue"""
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

class Queue:
	"""The queue is the main interfact you use to submit, query and manipulate
	jobs."""

	# FIXME: Add support for that
	MAX_JOBS       = 128000
	# The queue is cleaned up after a given period in seconds
	CLEANUP_PERIOD = 60 * 5

	def __init__( self ):
		self.pool      = Pool()
		self.jobs      = []
		self.incidents = []
		self._lastSelected = -1
		self.lastClean     = -1

	def setPool( self, pool ):
		"""Sets the job pool to be used"""
		# NOTE: Shouldn't we do something with the existing pool if jobs
		# are currently running?
		self.pool = pool
	
	def clean( self ):
		"""Executes periodic cleaning up operations on the queue"""
		# FIXME: WHat should be done here?
		self.lastClean = time.time()

	def clear( self ):
		"""Clears all the jobs and incidents from the queue"""
		map(self._removeJob, self.list())
		self.incidents = []

	def submit( self, job ):
		"""Submit a new job or a list of jobs in this queue"""
		if type(job) in (tuple, list):
			return map(self.submit, job)
		else:
			job.submitted = timestamp ()
			job.setStatus(JOB_SUBMITTED)
			return self._submitJob(job)

	def process( self, jobOrID):
		"""Processes the given job. This will mark it as in process and
		run it"""
		job = self._job(jobOrID)
		job.setStatus(JOB_IN_PROCESS)
		self._processJob(job)
		return self._runJob(job)
	
	def _runJob( self, job ):
		# Makes sure it's time to execute it
		# ...if not, we return the time we have to wait up until the next event
		# Makes sure the pool can process the event
		# ...if not, we return the maximum time in which the pool will be free/or a callback to when the pool will be free
		if not job.isUnresolved:
			if not self.pool:
				raise Exception("Workqueue has no associated worker pool (see `setPool`)")
			worker = self.pool.submit(job, block=True)
			# Now we have the worker and we ask it to run the job
			log(self.__class__.__name__, "IN PROCESS ", job)
			result = worker.run()
		else:
			err(self.__class__.__name__, "UNRESOLVED ", job)
			result  = Failure("Unresolved job")
		if   isinstance(result, Success):
			log(self.__class__.__name__, "SUCCESS    ", job, ":", result)
			job.retries = 0
			# the job is successfully processed
			self._onJobSucceeded(job, result)
		elif isinstance(result, Failure):
			self._onJobFailed(job, result)
		else:
			self._onJobFailed(job, UnexpectedResult(result))
		return result

	def resubmit( self, job ):
		assert job.id != None, "You cannot resumbit a job without an id: %s" % (job)
		# We increase the number of retries in the job
		job.retries += 1
		self.setStatus(JOB_RESUBMITTED)
		self._resubmitJob(job)
	
	def failure( self, job ):
		"""Removes the job from the queue after too many failures"""
		assert job.id != None
		self.setStatus(JOB_FAILED)
		self._failedJob(job)
		return job

	def complete( self, job ):
		"""Removes the job from the queue"""
		assert job.id != None
		self.setStatus(JOB_COMPLETED)
		self._completeJob(job)
		return job

	def remove( self, job ):
		"""Removes the job from the queue"""
		self.setStatus(JOB_REMOVED)
		self._removeJob(job)
		return job

	def list( self, until=None, since=None, status=None, queue=None ):
		"""Lists the jobs ids in the job queue, by ascending chronological order"""
		return  self._listJobs()

	def get( self, jobID ):
		"""Returns the Job instance with the given ID"""
		return self._getJob(jobID)


	def iterate( self, count=-1 ):
		while (count == -1 or count > 0) and self._hasJobs():
			# Takes the next available job
			job = self._getNextJob()
			log(self.__class__.__name__, "SUBMIT     ", job)
			result = self.process(job)
			if count > 0: count -= 1
			# Takes care of cleaning up the queue if it's necessary
			now = time.time()
			if self.lastClean == -1:
				self.lastClean = now
			elif (self.lastClean - now) > self.CLEANUP_PERIOD:
				self.clean()
				self.lastClean = now
			# And finally returns the result
			yield result

	def run( self, count=-1 ):
		"""Runs the workqueue for `count` interations, stopping when
		no more job is available."""
		iteration = 0
		for result in self.iterate(count):
			iteration += 1
		return iteration

	def _onJobSucceeded( self, jobOrJobID, result ):
		job = self._job(jobOrJobID)
		job.setStatus(JOB_COMPLETED)
		self._completeJob(job)

	def _onJobFailed( self, job, failure ):
		"""Called when a job has failed."""
		incident    = self._getIncident(job, failure)
		failure.job = job
		if incident.isAboveThreshold():
			# If the incident is above threshold, we won't retry the job,
			# as it's likely to fail again
			warn(self.__class__.__name__, "!INCIDENT", job.id, ":", incident, "with", failure)
			self.failure(job)
		elif job.canRetry():
			# If the incident is not above threshold, and the job has not
			# reached its retry count, we resubmit it
			warn(self.__class__.__name__, "!RESUBMIT", job.id, "/", job.retries, "because of", failure)
			self.resubmit(job)
		else:
			# Otherwise we remove the job from the queue
			warn(self.__class__.__name__, "!MAXRETRY", job.id, "/", job.retries, "after", failure)
			self.failure(job)
		return incident

	def _getIncident( self, job, failure ):
		"""Returns and incident that matches the job and failure"""
		for incident in self.incidents:
			if incident.log(job, failure):
				return incident
		incident = Incident(job,failure)
		self.incidents.append(incident)
		return incident

	# JOB-ID CONVERSIONS

	def _id( self, jobOrJobID ):
		if isinstance(jobOrJobID, Job):
			res = jobOrJobID.id
			assert res is not None, "Job has no id"
			return res
		else:
			return jobOrJobID 

	def _job( self, jobOrJobID ):
		if isinstance(jobOrJobID, Job):
			return jobOrJobID
		else:
			return self._getJob(jobOrJobID)

	# BACK-END SPECIFIC METHODS

	def _getNextJob( self ):
		"""Returns the next job and sets it as selected in this queue"""
		raise Exception("Not implemented yet")

	def _getJob( self, jobID ):
		"""Returns the job given its job id."""
		raise Exception("Not implemented yet")

	def _hasJobs( self ):
		"""Tells if there are still jobs submitted in the queue"""
		raise Exception("Not implemented yet")

	def _submitJob( self, job ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		raise Exception("Not implemented yet")

	def _processJob( self, job ):
		"""A job is being processed."""
		raise Exception("Not implemented yet")

	def _resubmitJob( self, job ):
		"""Adds an existing job to the queue and returns its ID (assigned by the queue)"""
		raise Exception("Not implemented yet")

	def _completeJob( self, job ):
		raise Exception("Not implemented yet")

	def _failedJob( self, job ):
		"""A job that has failed is archived and might be re-run later."""
		raise Exception("Not implemented yet")

	def _removeJob( self, job ):
		"""Removes a job from the queue, permanently"""
		raise Exception("Not implemented yet")


# -----------------------------------------------------------------------------
#
# MEMORY QUEUE
#
# -----------------------------------------------------------------------------

class MemoryQueue(Queue):

	def __init__( self ):
		Queue.__init__(self)
		self.jobs = []

	def _getNextJob( self ):
		"""Returns the next job and sets it as selected in this queue"""
		if self.jobs:
			return self.jobs[0]
		else:
			return None

	def _hasJobs( self ):
		"""Tells if there are still jobs submitted in the queue"""
		return self.jobs

	def _getJob( self, jobID ):
		for job in self.jobs:
			if job.id == jobID:
				return job
		return None

	def _submitJob( self, job ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		# FIXME: Should be synchronized
		self.jobs.append(job)
		return job.setID("%s@%s" % (len(self.jobs) - 1, time.time())).id

	def _resubmitJob( self, job ):
		"""Adds an existing job to the queue and returns its ID (assigned by the queue)"""
		# FIXME: Should be synchronized
		self.jobs.append(job)
		return job.id

	def _completeJob( self, job ):
		# NOTE: This is pretty simple, but should be optimized for big queues
		self.jobs.remove(self._job(job))

	def _failedJob( self, job ):
		# FIXME: A different strategy might be best
		self.jobs.remove(self._job(job))

# -----------------------------------------------------------------------------
#
# DIRECTORY QUEUE
#
# -----------------------------------------------------------------------------

class DirectoryQueue(Queue):

	# FIXME: Should use systems's directory watching

	SUFFIX      = ".json"
	QUEUES      = [QUEUE_INCOMING, QUEUE_IN_PROCESS, QUEUE_COMPLETED, QUEUE_FAILED]
	DIRECTORIES = {
		QUEUE_INCOMING   : QUEUE_INCOMING,
		QUEUE_IN_PROCESS : QUEUE_IN_PROCESS,
		QUEUE_FAILED     : QUEUE_FAILED,
		QUEUE_COMPLETED  : QUEUE_COMPLETED
	}

	def __init__( self, path ):
		Queue.__init__(self)
		self.path = path
		# We create the directory if it does not exist
		if not os.path.exists(path):
			os.makedirs(path)
		for queue in self.QUEUES:
			queue_path = path + "/" + queue
			if not os.path.exists(queue_path):
				os.makedirs(queue_path)

	def read( self, path, sync=True ):
		"""Atomically read the file at the given path"""
		flags = os.O_RDONLY
		if sync: flags = flags | os.O_RSYNC
		fd    = os.open(path, flags)
		data  = None
		try:
			last_read = 1 
			data      = []
			while last_read > 0:
				t = os.read(fd, 128000)
				data.append(t)
				last_read = len(t)
			data = "".join(data)
			os.close(fd)
		except StandardError, e:
			os.close(fd)
			raise e
		return data

	def write( self, data, path, sync=True, append=False ):
		"""Atomically write the given data in the file at the given path"""
		flags = os.O_WRONLY | os.O_CREAT
		if sync:       flags = flags | os.O_DSYNC
		if not append: flags = flags | os.O_TRUNC
		fd    = os.open(path, flags)
		try:
			os.write(fd, data)
			os.close(fd)
		except StandardError, e:
			os.close(fd)
			raise e
		return self

	def timestamp( self ):
		now = datetime.datetime.now()
		return "%04d%02d%02dT%02d:%02d%02d%d" % (
			now.year, now.month, now.day, now.hour, now.minute, now.second, now.microsecond
		)

	def _getPath( self, job, queue=QUEUE_INCOMING ):
		if isinstance(job, Job): job_id = job.id
		else: job_id = job
		return self.path + "/" + queue + "/" + job_id + self.SUFFIX

	def _listJobs( self, queue=QUEUE_INCOMING ):
		for _ in os.listdir(self.path + "/" + queue):
			if _.endswith(self.SUFFIX):
				yield _[:-len(self.SUFFIX)]

	def _getJob( self, jobID ):
		"""Returns the job given its job id."""
		for queue in self.QUEUES:
			path     = self._getPath(jobID, queue)
			if os.path.exists(path):
				job = json.loads(self.read(path))
				job = Job.Import(job, jobID)
				return job
		return None

	def _getNextJob( self, queue=QUEUE_INCOMING ):
		"""Returns the next job and sets it as selected in this queue"""
		iterator = self._listJobs()
		job      = iterator.next()
		return self._getJob(job)

	def _hasJobs( self, queue=QUEUE_INCOMING ):
		"""Tells if there are still jobs submitted in the queue"""
		iterator = self._listJobs()
		try:
			iterator.next()
			return True
		except StopIteration, e:
			return False

	def _submitJob( self, job ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		new_id = self.timestamp() + "-" + job.__class__.__name__
		job.setID(new_id)
		self.write(asJSON(job.export()), self._getPath(job, QUEUE_INCOMING))
		return job.id

	def _processJob( self, job ):
		self._removeJobFile(job)
		self.write(asJSON(job.export()), self._getPath(job, QUEUE_IN_PROCESS))
		return job.id

	def _submitJob( self, job ):
		"""Adds a new job to the queue and returns its ID (assigned by the queue)"""
		new_id = self.timestamp() + "-" + job.__class__.__name__
		job.setID(new_id)
		self.write(asJSON(job.export()), self._getPath(job, QUEUE_INCOMING))
		return job.id

	def _resubmitJob( self, job ):
		"""Adds an existing job to the queue and returns its ID (assigned by the queue)"""
		self._removeJobFile(job)
		self.write(asJSON(job.export()), self._getPath(job, QUEUE_INCOMING))
		return job.id

	def _completeJob( self, job ):
		self._removeJobFile(job)
		self.write(asJSON(job.export()), self._getPath(job, QUEUE_COMPLETED))
		return job.id

	def _failedJob( self, job ):
		self._removeJobFile(job)
		self.write(asJSON(job.export()), self._getPath(job, QUEUE_FAILED))
		return job.id

	def _removeJob( self, job ):
		self._removeJobFile(job)

	def _removeJobFile( self, job ):
		"""Physically removes the job file."""
		path = self._getPath(job)
		if os.path.exists(path): os.unlink(path)

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

# -----------------------------------------------------------------------------
#
# DAEMON
#
# -----------------------------------------------------------------------------

class Daemon:
	"""A very simple class that wraps a queue and runs it, sleep and waking
	up every second to check for more work."""

	def __init__( self, queue=None, period=1 ):
		self.queue       = queue
		self.isRunning   = False
		self.sleepPeriod = period
	
	def run( self ):
		self.isRunning = True
		while self.isRunning:
			if self.queue.run(1) == 0 and self.isRunning:
				time.sleep(self.sleepPeriod)
	
	def stop( self ):
		self.isRunning = False

# EOF - vim: tw=80 ts=4 sw=4 noet
